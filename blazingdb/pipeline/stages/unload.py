"""
Defines the series of stages for handling unloads from Redshift
"""

import asyncio
import contextlib
import csv
import json
import logging
import os
import types

import botocore.session
import pandas

from blazingdb.util import process, s3

from . import base
from .. import packets
from ..util import get_columns


# pragma pylint: disable=too-few-public-methods

UNLOAD_DELIMITER = "|"
TYPE_MAP = {
    "long": "int64",
    "double": "float64",
    "string": "str"
}

def retrieve_unloaded_file(bucket, key, access_key, secret_key, columns, chunk_size):
    """ Retrieves an unloaded file from S3 into a pandas.DataFrame """
    import gc

    session = botocore.session.get_session()
    client = session.create_client("s3",
        aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    stream, _ = s3.open_file(client, bucket, key)
    stream = _attach_iter_method(stream, chunk_size)

    gc.collect()

    with contextlib.closing(stream):
        names = [column.name for column in columns]
        date_cols = [i for i, column in enumerate(columns) if column.type == "date"]

        dtypes = {
            column.name: TYPE_MAP[column.type]
            for column in columns if column.type in TYPE_MAP}

        return pandas.read_csv(stream, sep=UNLOAD_DELIMITER,
            lineterminator="\n", quotechar=None, quoting=csv.QUOTE_NONE,
            doublequote=False, escapechar="\\", na_values="",
            names=names, dtype=dtypes, parse_dates=date_cols,
            infer_datetime_format=True, engine="c",
            keep_default_na=False)

def _attach_iter_method(stream, chunk_size):
    def _iter(target):
        while True:
            data = target.read(chunk_size)

            if not data:
                break

            yield data

    stream.__iter__ = types.MethodType(_iter, stream)

    return stream


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
        query_columns = ",".join(column.name for column in columns)

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

    DEFAULT_CHUNK_SIZE = 65536
    DEFAULT_PENDING_HANDLES = os.cpu_count() / 2

    def __init__(self, access_key, secret_key, loop=None, **kwargs):
        super(UnloadRetrievalStage, self).__init__(packets.DataUnloadPacket)
        self.logger = logging.getLogger(__name__)
        self.loop = loop

        self.access_key = access_key
        self.secret_key = secret_key

        self.client = botocore.session.get_session().create_client("s3",
            aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        self.executor = process.ProcessPoolExecutor(process.quiet_sigint)

        self.chunk_size = kwargs.get("chunk_size", self.DEFAULT_CHUNK_SIZE)
        self.pending_handles = kwargs.get("pending_handles", self.DEFAULT_PENDING_HANDLES)

    async def _limit_pending(self, pending):
        if len(pending) <= self.pending_handles:
            return pending

        pending_iter = asyncio.as_completed(pending, loop=self.loop)
        for future in pending_iter:
            await future

            pending = [handle for handle in pending if not handle.done()]

            if len(pending) <= self.pending_handles:
                break

        return pending

    async def _read_manifest(self, bucket, key):
        stream, _ = s3.open_file(self.client, bucket, key)

        with contextlib.closing(stream):
            manifest_json = json.loads(stream.read())

        return [entry["url"] for entry in manifest_json["entries"]]

    async def _retrieve_file(self, url, columns):
        bucket, key = s3.parse_url(url)
        self.logger.info("Retrieving unloaded file: %s", key)

        return await self.loop.run_in_executor(self.executor, retrieve_unloaded_file,
            bucket, key, self.access_key, self.secret_key, columns, self.chunk_size)

    async def process(self, message):
        unload_pkt = message.pop_packet(packets.DataUnloadPacket)
        manifest = unload_pkt.key + "manifest"

        urls = await self._read_manifest(unload_pkt.bucket, manifest)
        columns = await get_columns(message)

        pending = []
        for i, url in enumerate(urls):
            pending = await self._limit_pending(pending)
            frame = await self._retrieve_file(url, columns)

            packet = packets.DataFramePacket(frame, i)
            handle = await message.forward(packet, track_children=True)

            pending.append(handle)

        if pending:
            await asyncio.wait(pending, loop=self.loop)

        await message.forward(packets.DataCompletePacket())
