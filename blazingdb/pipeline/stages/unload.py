"""
Defines the series of stages for handling unloads from Redshift
"""

import asyncio
import cgi
import collections
import csv
import datetime
import json
import logging
import re

from . import base
from .. import packets


# pragma pylint: disable=too-few-public-methods

PYTHON_DATE_FORMAT = "%Y-%m-%d"
UNLOAD_DATE_FORMAT = "YYYY-MM-DD"
UNLOAD_DELIMITER = "|"

class S3ReadTransport(asyncio.ReadTransport):
    """ Custom asyncio.ReadTransport for reading from a StreamingResponse """

    DEFAULT_BUFFER_AMOUNT = 4096
    DEFAULT_CHARSET = "utf-8"

    get_protocol = None
    set_protocol = None

    def __init__(self, reader, response, loop=None, **kwargs):
        super(S3ReadTransport, self).__init__()

        loop = loop if loop is not None else asyncio.get_event_loop()
        self.task = asyncio.ensure_future(self._read_stream(reader), loop=loop)

        _, type_params = cgi.parse_header(response["ContentType"])
        self.encoding = type_params.get("charset", S3ReadTransport.DEFAULT_CHARSET)

        self.is_closed = False
        self.buffer_amount = kwargs.get("buffer_amount", S3ReadTransport.DEFAULT_BUFFER_AMOUNT)
        self.waiter = asyncio.Event(loop=loop)
        self.stream = response["Body"]

        self.resume_reading()

    async def _read_stream(self, reader):
        while True:
            await self.waiter.wait()

            data = await self.stream.read(self.buffer_amount)

            if not data:
                break

            reader.feed_data(data)

        reader.feed_eof()
        self.close()

    def close(self):
        self.stream.close()
        self.is_closed = True

    def is_closing(self):
        return self.is_closed

    def pause_reading(self):
        self.waiter.clear()

    def resume_reading(self):
        self.waiter.set()


class UnloadSliceStream(object):
    """ Reads rows out of a series of unloaded slices of data from S3 """

    def __init__(self, client, urls, loop=None):
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.urls = collections.deque(urls)
        self.client = client

        self.current_reader = None
        self.current_transport = None

    @staticmethod
    def _parse_s3_url(url):
        match = re.match("s3://([a-z0-9,.-]+)/(.*)", url)
        return match.group(1, 2) if match else None

    def _create_reader(self, response):
        """ Creates an asyncio.StreamReader for reading the given response """
        reader = asyncio.StreamReader(loop=self.loop)
        transport = S3ReadTransport(reader, response, loop=self.loop)
        reader.set_transport(transport)

        return reader, transport

    async def _retrieve_object(self, bucket, key):
        """ Retrieves a stream for the object """
        return await self.client.get_object(Bucket=bucket, Key=key)

    async def _populate_reader(self):
        """ Parses the next url in the queue and creates a reader from it """
        bucket, key = self._parse_s3_url(self.urls.popleft())
        response = await self._retrieve_object(bucket, key)
        reader, transport = self._create_reader(response)

        self.current_reader = reader
        self.current_transport = transport

    def at_eof(self):
        """ Determines whether or not there is any futher data in the slices """
        return not self.urls and self.current_reader.at_eof()

    async def readline(self):
        """ Reads the next line from the stream """
        if self.current_reader is None:
            await self._populate_reader()

        while self.urls and self.current_reader.at_eof():
            await self._populate_reader()

        line = await self.current_reader.readline()
        line = line.decode(self.current_transport.encoding)

        return line

class UnloadStreamReader(object):
    """ Reads lines from an UnloadSliceStream and processes them into rows """

    class Dialect(csv.Dialect):
        """ The dialect used by the Amazon Redshift UNLOAD command """
        delimiter = UNLOAD_DELIMITER
        doublequote = False
        escapechar = "\\"
        lineterminator = "\n"
        quotechar = None
        quoting = csv.QUOTE_NONE
        skipinitialspace = False
        strict = True

    class Queue(object):
        """ An interable queue for consumption by csv.reader """

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

    DEFAULT_QUEUE_BUFFER = 24000

    def __init__(self, stream, **kwargs):
        self.stream = stream

        self.queue_buffer = kwargs.get("queue_buffer", UnloadStreamReader.DEFAULT_QUEUE_BUFFER)
        self.queue = UnloadStreamReader.Queue()

        self.csv_reader = csv.reader(
            self.queue, dialect=UnloadStreamReader.Dialect())

    async def _read_stream(self):
        """ Reads the next line from the stream """
        return await self.stream.readline()

    async def _populate_queue(self):
        """ Populates the internal queue from the stream """
        lines = []
        for _ in range(0, self.queue_buffer):
            if self.stream.at_eof():
                break

            line = await self._read_stream()

            if not line:
                continue

            lines.append(line)

        self.queue.push(lines)

    async def readrow(self):
        """ Reads the next row from the stream """
        if self.queue.empty():
            if self.stream.at_eof():
                return None

            await self._populate_queue()

        return next(self.csv_reader)


class UnloadGenerationStage(base.BaseStage):
    """ Performs an UNLOAD query on Redshift to export data for a table """

    def __init__(self, bucket, access_key, secret_key, path_prefix=None, session_token=None): # pylint: disable=too-many-arguments
        super(UnloadGenerationStage, self).__init__(packets.ImportTablePacket)
        self.logger = logging.getLogger(__name__)

        self.bucket = bucket
        self.path_prefix = path_prefix

        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token

    @staticmethod
    def _build_query_column(column):
        if column.type == "date":
            return "to_char({0}, '{1}')".format(column.name, UNLOAD_DATE_FORMAT)

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

        self.logger.debug("Unloading data from Redshift with query, %s", query)

        await source.execute(" ".join([
            "UNLOAD ('{0}')".format(query.replace("'", "''")),
            "TO 's3://{0}/{1}'".format(self.bucket, key),
            "MANIFEST ALLOWOVERWRITE ESCAPE",
            "DELIMITER AS '{0}'".format(UNLOAD_DELIMITER),
            self._generate_credentials(),
        ]))

        await message.forward()


class UnloadProcessingStage(base.BaseStage):
    """ Processes a DataUnloadPacket and transforms it into a stream of DataLoadPacket """

    DEFAULT_BATCH_COUNT = 24000
    DEFAULT_PENDING_HANDLES = 10

    def __init__(self, client, loop=None, **kwargs):
        super(UnloadProcessingStage, self).__init__(packets.DataUnloadPacket)
        self.logger = logging.getLogger(__name__)

        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.client = client

        self.batch_count = kwargs.get("batch_count", UnloadProcessingStage.DEFAULT_BATCH_COUNT)
        self.pending_handles = kwargs.get("pending_handles", UnloadProcessingStage.DEFAULT_PENDING_HANDLES)

    @staticmethod
    def _process_data(column, data):
        # pragma pylint: disable=multiple-statements
        if not data: return None
        elif column.type == "long": return int(data)
        elif column.type == "double": return float(data)
        elif column.type == "string": return data
        elif column.type == "date":
            return datetime.datetime.strptime(data, PYTHON_DATE_FORMAT)

        raise ValueError("unknown column type, {0}".format(column.type))

    async def _read_batch(self, reader, columns):
        rows = []

        for _ in range(0, self.batch_count):
            row = await reader.readrow()

            if row is None:
                break

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

        stream = UnloadSliceStream(self.client, urls, loop=self.loop)
        reader = UnloadStreamReader(stream)

        index = 0
        handles = []
        while True:
            data = await self._read_batch(reader, columns_pkt.columns)

            if not data:
                break

            packet = packets.DataLoadPacket(data, index)
            handle = await message.forward(packet, track_children=True)

            handles.append(handle)
            index += 1

            while len(handles) >= self.pending_handles:
                _, pending = await asyncio.wait(
                    handles, loop=self.loop,
                    return_when=asyncio.FIRST_COMPLETED)

                handles = list(pending)

        await asyncio.wait(handles, loop=self.loop)
        await message.forward(packets.DataCompletePacket())
