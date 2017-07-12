"""
Defines the series of stages for handling unloads from Redshift
"""

import asyncio
import codecs
import collections
import csv
import datetime
import json
import re

from . import base
from .. import packets


class S3Transport(asyncio.ReadTransport):
    """ Custom asyncio.ReadTransport for reading from a StreamingResponse """
    Utf8Decoder = codecs.getincrementaldecoder("utf-8")

    get_protocol = None
    set_protocol = None

    def __init__(self, reader, stream, loop=None):
        super(S3Transport, self).__init__()

        loop = loop if loop is not None else asyncio.get_event_loop()
        asyncio.ensure_future(self._read_stream(reader, stream), loop=loop)

        self.is_closed = False
        self.stream = stream

        self.buffer_amount = 4096
        self.waiter = asyncio.Event(loop=loop)
        self.resume_reading()

    async def _read_stream(self, reader, stream):
        while True:
            await self.waiter.wait()

            data = await stream.read(self.buffer_amount)

            # pragma pylint: disable=multiple-statements
            if not data: break

            reader.feed_data(data)

        reader.feed_eof()
        stream.close()

    def close(self):
        self.stream.close()

    def is_closing(self):
        return self.is_closed

    def pause_reading(self):
        self.waiter.clear()

    def resume_reading(self):
        self.waiter.set()


class QueueIterator(object):
    def __init__(self):
        self.queue = collections.deque()

    def __iter__(self):
        return self

    def __next__(self):
        if not self.queue:
            raise StopIteration

        return self.queue.popleft()

    def empty(self):
        return not bool(self.queue)

    def push(self, elements):
        self.queue.extend(elements)
        return self


class UnloadStream(object):
    """ Chains a series of unloaded slices into an iterable stream """

    def __init__(self, client, urls, loop=None):
        self.loop = loop if loop is not None else asyncio.get_event_loop()

        self.client = client
        self.current_reader = None
        self.urls = collections.deque(urls)

    @staticmethod
    def _parse_s3_url(url):
        match = re.match("s3://([a-z0-9,.-]+)/(.*)", url)
        return match.group(1, 2) if match else None

    def _create_reader(self, stream):
        reader = asyncio.StreamReader(loop=self.loop)
        transport = S3Transport(reader, stream, loop=self.loop)

        reader.set_transport(transport)
        return reader

    async def _create_stream(self, bucket, key):
        response = await self.client.get_object(Bucket=bucket, Key=key)
        return response["Body"]

    async def _populate_reader(self):
        bucket, key = self._parse_s3_url(self.urls.popleft())
        stream = await self._create_stream(bucket, key)
        reader = self._create_reader(stream)

        self.current_reader = reader

    def at_eof(self):
        return not self.urls and self.current_reader.at_eof()

    async def readline(self):
        if self.current_reader is None:
            await self._populate_reader()

        while self.urls and self.current_reader.at_eof():
            await self._populate_reader()

        return await self.current_reader.readline()

class UnloadReader(object):

    class Dialect(csv.Dialect):
        delimiter = "|"
        doublequote = False
        escapechar = "\\"
        lineterminator = "\n"
        quotechar = None
        quoting = csv.QUOTE_NONE
        skipinitialspace = False
        strict = True

    def __init__(self, stream):
        self.stream = stream
        self.queue = QueueIterator()

        self.csv_reader = csv.reader(
            self.queue, dialect=UnloadReader.Dialect())

    async def _read_stream(self):
        line = await self.stream.readline()
        line = line.decode("utf-8")

        return line

    async def _populate_queue(self):
        lines = []
        for _ in range(0, 1000):
            if self.stream.at_eof():
                break

            line = await self._read_stream()

            if not line: continue
            lines.append(line)

        self.queue.push(lines)

    async def readrow(self):
        if self.queue.empty():
            if self.stream.at_eof():
                return None

            await self._populate_queue()

        return next(self.csv_reader)


class UnloadGenerationStage(base.BaseStage):
    """ Performs an UNLOAD query on Redshift to export data for a table """

    def __init__(self, bucket, access_key, secret_key, path_prefix=None, session_token=None):
        super(UnloadGenerationStage, self).__init__(packets.ImportTablePacket)

        self.bucket = bucket
        self.path_prefix = path_prefix

        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token

    @staticmethod
    def _build_query_column(column):
        if column.type == "date":
            return "to_char({0}, 'YYYY-MM-DD')".format(column.name)

        return column.name

    def _generate_credentials(self):
        segments = [
            "ACCESS_KEY_ID '{0}'".format(self.access_key),
            "SECRET_ACCESS_KEY '{0}'".format(self.secret_key)
        ]

        if self.session_token is not None:
            segments.append("SESSION_TOKEN '{0}'".format(self.session_token))

        return " ".join(segments)

    async def process(self, message):
        import_pkt = message.get_packet(packets.ImportTablePacket)

        source = import_pkt.source
        table = import_pkt.table

        key = table + "/slice_"
        if self.path_prefix is not None:
            key = self.path_prefix + "/" + key

        columns = await source.get_columns(table)
        query_columns = ",".join(map(self._build_query_column, columns))

        message.add_packet(packets.DataColumnsPacket(columns))
        message.add_packet(packets.DataUnloadPacket(self.bucket, key))

        query = " ".join([
            "SELECT {0}".format(query_columns),
            "FROM {0}".format(source.get_identifier(table))
        ])

        await source.execute(" ".join([
            "UNLOAD ('{0}')".format(query.replace("'", "''")),
            "TO 's3://{0}/{1}'".format(self.bucket, key),
            "MANIFEST ALLOWOVERWRITE ESCAPE",
            "DELIMITER AS '{0}'".format("|"),
            self._generate_credentials(),
        ]))

        await message.forward()


class UnloadProcessingStage(base.BaseStage):
    """ Processes a DataUnloadPacket and transforms it into a stream of DataLoadPacket """

    def __init__(self, client, loop=None):
        super(UnloadProcessingStage, self).__init__(packets.DataUnloadPacket)
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.client = client

    @staticmethod
    def _process_data(column, data):
        if column.type == "long":
            return int(data)
        elif column.type == "double":
            return float(data)
        elif column.type == "string":
            return data
        elif column.type == "date":
            return datetime.datetime.strptime(data, "%Y-%m-%d")

        raise ValueError("unknown column type, {0}".format(column.type))

    async def _read_batch(self, reader, columns):
        rows = []
        for _ in range(0, 1000):
            row = await reader.readrow()
            if row is None: break

            if len(row) != len(columns):
                print(row, columns)

            for i, column in enumerate(columns):
                row[i] = self._process_data(column, row[i])

            rows.append(row)

        return rows

    async def _read_manifest(self, bucket, key):
        response = await self.client.get_object(Bucket=bucket, Key=key)

        async with response["Body"] as stream:
            manifest_json = json.loads(await stream.read())

        return [entry["url"] for entry in manifest_json["entries"]]

    async def process(self, message):
        unload_pkt = message.pop_packet(packets.DataUnloadPacket)
        columns_pkt = message.get_packet(packets.DataColumnsPacket)

        manifest = unload_pkt.key + "manifest"
        urls = await self._read_manifest(unload_pkt.bucket, manifest)

        stream = UnloadStream(self.client, urls, loop=self.loop)
        reader = UnloadReader(stream)

        index = 0
        handles = []
        while True:
            data = await self._read_batch(reader, columns_pkt.columns)
            if not data: break

            packet = packets.DataLoadPacket(data, index)
            handle = await message.forward(packet, track_children=True)

            handles.append(handle)
            index += 1

            while len(handles) >= 10:
                _, pending = await asyncio.wait(
                    handles, loop=self.loop,
                    return_when=asyncio.FIRST_COMPLETED)

                handles = list(pending)

        await asyncio.wait(handles, loop=self.loop)
        await message.forward(packets.DataCompletePacket())
