"""
Defines the series of stages for handling unloads from Redshift
"""

import asyncio
import codecs
import collections
import csv
import datetime
import functools
import hashlib
import json
import logging
import os.path
import signal
import tempfile

import aiofiles

from blazingdb.util import process, s3

from . import base
from .. import packets
from ..util import get_columns


# pragma pylint: disable=too-few-public-methods

PYTHON_DATE_FORMAT = "%Y-%m-%d"
UNLOAD_DATE_FORMAT = "YYYY-MM-DD"
UNLOAD_DELIMITER = "|"

async def buffer_s3_file(client, url, loop=None, **kwargs):
    """ Buffers an S3 file locally to allow lazy reading of data """
    hasher = hashlib.sha1()
    hasher.update(url.encode("utf-8"))
    url_hash = hasher.hexdigest()

    temp_dir = tempfile.TemporaryDirectory()
    temp_filename = os.path.join(temp_dir.name, url_hash)

    buffer_file = await aiofiles.open(temp_filename, "w+")
    reader, transport = await s3.open_s3(client, url, loop=loop, **kwargs)

    await buffer_file.truncate()
    while True:
        line = await reader.readline()
        line = line.decode(transport.get_encoding())

        if not line:
            break

        await buffer_file.write(line)

    await buffer_file.seek(0)
    return buffer_file, temp_dir


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
        self.client = client
        self.loop = loop

        self.batch_count = kwargs.get(
            "batch_count", UnloadRetrievalStage.DEFAULT_BATCH_COUNT)
        self.pending_handles = kwargs.get(
            "pending_handles", UnloadRetrievalStage.DEFAULT_PENDING_HANDLES)

    async def _limit_pending(self, pending):
        if len(pending) <= self.pending_handles:
            return pending

        pending_iter = asyncio.as_completed(pending, loop=self.loop)
        for future in pending_iter:
            await future

            if len(pending) <= self.pending_handles:
                break

        return [handle for handle in pending if not handle.done()]

    async def _read_batch(self, stream, transport):
        lines = []

        for _ in range(0, self.batch_count):
            line = await stream.readline()
            line = line.decode(transport.get_encoding())

            if not line:
                break

            lines.append(line)

        return lines

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
        pending = []

        while urls:
            await self._limit_pending(pending)

            s3_file, transport = await s3.open_s3(self.client, urls.popleft(), loop=self.loop)
            while True:
                batch = await self._read_batch(s3_file, transport)

                if not batch:
                    break

                packet = packets.DataLoadPacket(batch, index)
                handle = await message.forward(packet, track_children=True)

                pending.append(handle)
                index += 1

        if pending:
            await asyncio.wait(pending, loop=self.loop)

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
