"""
Defines the series of stages for handling unloads from Redshift
"""

import asyncio
import cgi
import collections
import csv
import datetime
import functools
import json
import logging
import os
import re
import signal

from blazingdb.util import process, timer

from . import base
from .. import packets
from ..util import get_columns


# pragma pylint: disable=too-few-public-methods

PYTHON_DATE_FORMAT = "%Y-%m-%d"
UNLOAD_DATE_FORMAT = "YYYY-MM-DD"
UNLOAD_DELIMITER = "|"

class UnloadDialect(csv.Dialect):
    """ The dialect used by the Amazon Redshift UNLOAD command """
    delimiter = UNLOAD_DELIMITER
    doublequote = False
    escapechar = "\\"
    lineterminator = "\n"
    quotechar = None
    quoting = csv.QUOTE_NONE
    skipinitialspace = False
    strict = True


class UnloadStream(object):
    """ Reads rows out of a series of unloaded slices of data from S3 """

    def __init__(self, response, loop=None):
        loop = loop if loop is not None else asyncio.get_event_loop()
        self.reader, self.transport = self._create_reader(response, loop)

    @classmethod
    async def create_stream(cls, client, url, loop=None):
        """ Creates an UnloadStream for the given s3 url """
        bucket, key = cls._parse_s3_url(url)

        logger = logging.getLogger(__name__)
        logger.info("Starting to process unloaded chunk: %s", key)

        response = await client.get_object(Bucket=bucket, Key=key)
        return cls(response, loop=loop)

    @staticmethod
    def _parse_s3_url(url):
        match = re.match("s3://([a-z0-9,.-]+)/(.*)", url)
        return match.group(1, 2) if match else None

    @staticmethod
    def _create_reader(response, loop):
        """ Creates an asyncio.StreamReader for reading the given item """
        reader = asyncio.StreamReader(loop=loop)

        transport = S3ReadTransport(reader, response, loop=loop)
        reader.set_transport(transport)

        return reader, transport

    def at_eof(self):
        """ Determines whether or not there is any futher data in the slices """
        return self.reader.at_eof()

    async def readline(self):
        """ Reads the next line from the stream """
        line = await self.reader.readline()
        line = line.decode(self.transport.encoding)

        return line


class S3ReadTransport(asyncio.ReadTransport):
    """ Custom asyncio.ReadTransport for reading from a StreamingResponse """

    DEFAULT_BUFFER_AMOUNT = 65536
    DEFAULT_CHARSET = "utf-8"
    DEFAULT_TIMER_INTERVAL = 5

    get_protocol = None
    set_protocol = None

    def __init__(self, reader, response, loop=None, **kwargs):
        super(S3ReadTransport, self).__init__()
        self.logger = logging.getLogger(__name__)

        loop = loop if loop is not None else asyncio.get_event_loop()
        buffer_amount = kwargs.get("buffer_amount", S3ReadTransport.DEFAULT_BUFFER_AMOUNT)
        self._start_reader(loop, reader, response["Body"], buffer_amount)

        _, type_params = cgi.parse_header(response["ContentType"])
        self.encoding = type_params.get("charset", S3ReadTransport.DEFAULT_CHARSET)
        self.waiter = asyncio.Event(loop=loop)
        self.is_closed = False

        timer_interval = kwargs.get("timer_interval", S3ReadTransport.DEFAULT_TIMER_INTERVAL)
        self.timer = timer.RepeatedTimer(timer_interval, self._print_state, loop=loop)
        self.last_checked = self.was_resumed = False

        self.resume_reading()

    def _start_reader(self, loop, reader, stream, buffer_amount):
        coroutine = self._safe_read_stream(reader, stream, buffer_amount)
        asyncio.ensure_future(coroutine, loop=loop)

    def _print_state(self):
        if self.was_resumed != self.last_checked:
            state = "resumed in" if self.was_resumed else "paused for"

            self.logger.debug("S3ReadTransport was %s the last %s seconds: %s",
                state, self.timer.interval, id(self))

        self.last_checked = self.was_resumed
        self.was_resumed = False

    async def _read_stream(self, reader, stream, buffer_amount):
        while True:
            await self.waiter.wait()

            if self.is_closed:
                break

            data = await stream.read(buffer_amount)

            if not data:
                break

            reader.feed_data(data)

    async def _safe_read_stream(self, reader, stream, buffer_amount):
        self.timer.start()

        try:
            await self._read_stream(reader, stream, buffer_amount)
        except:
            self.logger.exception("Failed reading from S3ReadTransport: %s", id(self))
            raise
        finally:
            self.timer.stop()

            reader.feed_eof()
            stream.close()

    def close(self):
        self.is_closed = True

    def is_closing(self):
        return self.is_closed

    def pause_reading(self):
        self.waiter.clear()

    def resume_reading(self):
        self.was_resumed = True
        self.waiter.set()


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

        columns = await get_columns(message, add_if_missing=True)
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


class UnloadRetrievalStage(base.BaseStage):
    """ Processes a DataUnloadPacket and transforms it into a stream of DataLoadPacket """

    DEFAULT_BATCH_COUNT = 10000
    DEFAULT_PENDING_HANDLES = 4

    def __init__(self, client, loop=None, **kwargs):
        super(UnloadRetrievalStage, self).__init__(packets.DataUnloadPacket)
        self.logger = logging.getLogger(__name__)

        self.loop = loop
        self.client = client

        self.batch_count = kwargs.get(
            "batch_count", UnloadRetrievalStage.DEFAULT_BATCH_COUNT)
        self.pending_handles = kwargs.get(
            "pending_handles", UnloadRetrievalStage.DEFAULT_PENDING_HANDLES)

    async def _read_batch(self, stream):
        rows = []

        for _ in range(0, self.batch_count):
            row = await stream.readline()

            if not row:
                break

            rows.append(row)

        return rows

    async def _read_manifest(self, bucket, key):
        response = await self.client.get_object(Bucket=bucket, Key=key)

        async with response["Body"] as stream:
            manifest_json = json.loads(await stream.read())

        return collections.deque(entry["url"] for entry in manifest_json["entries"])

    async def process(self, message):
        unload_pkt = message.pop_packet(packets.DataUnloadPacket)
        manifest = unload_pkt.key + "manifest"

        urls = await self._read_manifest(unload_pkt.bucket, manifest)

        index = 0
        handles = []
        while urls:
            stream = await UnloadStream.create_stream(self.client, urls.popleft(), loop=self.loop)

            while not stream.at_eof():
                data = await self._read_batch(stream)

                if not data:
                    break

                packet = packets.DataLoadPacket(data, index)
                handle = await message.forward(packet, track_children=True)

                handles.append(handle)
                index += 1

            pending_iter = asyncio.as_completed(handles, loop=self.loop)
            for future in pending_iter:
                if len(handles) <= self.pending_handles:
                    break

                await future

            handles = [handle for handle in handles if handle.done()]

        if handles:
            await asyncio.wait(handles, loop=self.loop)

        await message.forward(packets.DataCompletePacket())


class UnloadProcessingStage(base.BaseStage):
    """ Processes a DataUnloadPacket and transforms it into a stream of DataLoadPacket """

    def __init__(self, loop=None):
        super(UnloadProcessingStage, self).__init__(packets.DataLoadPacket)
        self.logger = logging.getLogger(__name__)

        self.executor = process.ProcessPoolExecutor(_quiet_sigint)
        self.loop = loop if loop is not None else asyncio.get_event_loop()

    async def shutdown(self):
        self.executor.shutdown(wait=True)

    async def _process_in_executor(self, data, columns):
        return await self.loop.run_in_executor(self.executor, process_data, data, columns)

    async def process(self, message):
        columns = message.get_packet(packets.DataColumnsPacket).columns

        for load_pkt in message.get_packets(packets.DataLoadPacket):
            processed_data = await self._process_in_executor(load_pkt.data, columns)
            message.update_packet(load_pkt, data=processed_data)

        await message.forward()


def process_data(data, columns):
    """ Processes the csv data resulting from an UNLOAD """
    reader = csv.reader(data, dialect=UnloadDialect())
    mappings = [_create_mapping(col) for col in columns]
    process_row = functools.partial(_process_row, mappings)

    return list(map(process_row, reader))

def _create_mapping(column):
    if column.type == "long":
        return int
    elif column.type == "double":
        return float
    elif column.type == "string":
        return str
    elif column.type == "date":
        return _parse_date

    raise ValueError("unknown column type, {0}".format(column.type))

def _parse_date(data):
    return datetime.datetime.strptime(data, PYTHON_DATE_FORMAT)

def _process_row(mappings, row):
    def _map_column(func, column):
        return func(column) if column else None

    return list(map(_map_column, mappings, row))

def _quiet_sigint():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
