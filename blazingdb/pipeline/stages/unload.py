"""
Defines the series of stages for handling unloads from Redshift
"""

import asyncio
import codecs
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


class UnloadGenerationStage(base.BaseStage):
    """ Performs an UNLOAD query on Redshift to export data for a table """

    def __init__(self, bucket, access_key, secret_key, path_prefix=None, session_token=None):
        super(UnloadGenerationStage, self).__init__(packets.ImportTablePacket)

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

        columns = await source.get_columns(table)
        query_columns = ",".join(column.name for column in columns)

        message.add_packet(packets.DataColumnsPacket(columns))
        message.add_packet(packets.DataUnloadPacket(self.bucket, key))

        query = " ".join([
            "SELECT {0}".format(query_columns),
            "FROM {0}".format(source.get_identifier(table))
        ])

        await source.execute(" ".join([
            "UNLOAD ('{0}')".format(query.replace("'", "''")),
            "TO 's3://{0}/{1}'".format(self.bucket, key),
            "MANIFEST ALLOWOVERWRITE {0}".format(self._generate_credentials())
        ]))

        await message.forward()


class UnloadProcessingStage(base.BaseStage):
    """ Processes a DataUnloadPacket and transforms it into a stream of DataLoadPacket """

    def __init__(self, client, loop=None):
        super(UnloadProcessingStage, self).__init__(packets.DataUnloadPacket)
        self.loop = loop if loop is not None else asyncio.get_event_loop()

        self.client = client
        self.buffer_amount = 10000

    @staticmethod
    def _parse_s3_url(url):
        regex = "s3://([a-z0-9,.-]+)/(.*)"
        match = re.match(regex, url)

        if not match:
            return None

        return match.group(1, 2)

    async def _batch_generator(self):
        data = []
        index = 0

        while True:
            reader = yield

            if reader is None:
                break

            while True:
                line = await reader.readline()
                line = line.decode("utf-8")

                if not line:
                    break

                data.append(line)

                if len(data) >= self.buffer_amount:
                    yield (data, index)

                    index += 1
                    data = []

        yield (data, index)

    async def _create_stream(self, bucket, key):
        return (await self.client.get_object(Bucket=bucket, Key=key))["Body"]

    async def _read_manifest(self, bucket, key):
        response = await self.client.get_object(Bucket=bucket, Key=key)

        async with response["Body"] as stream:
            manifest_json = json.loads(await stream.read())

        return [entry["url"] for entry in manifest_json["entries"]]

    async def process(self, message):
        unload_pkt = message.pop_packet(packets.DataUnloadPacket)

        generator = self._batch_generator()
        await generator.asend(None)

        handles = []
        manifest = unload_pkt.key + "manifest"
        for url in await self._read_manifest(unload_pkt.bucket, manifest):
            bucket, key = self._parse_s3_url(url)

            stream = await self._create_stream(bucket, key)
            reader = asyncio.StreamReader(loop=self.loop)
            transport = S3Transport(reader, stream, loop=self.loop)
            reader.set_transport(transport)

            batch = await generator.asend(reader)
            while batch is not None:
                data, index = batch

                packet = packets.DataLoadPacket(data, index)
                handle = await message.forward(packet, track_children=True)

                while len(handles) >= 10:
                    _, pending = await asyncio.wait(
                        handles, loop=self.loop,
                        return_when=asyncio.FIRST_COMPLETED
                    )

                    handles = list(pending)

                batch = await generator.asend(None)
                handles.append(handle)

        data, index = await generator.asend(None)

        if data:
            packet = packets.DataLoadPacket(data, index)
            handle = await message.forward(packet, track_children=True)

            handles.append(handle)

        await asyncio.wait(handles + [handle], loop=self.loop)
        await message.forward(packets.DataCompletePacket())
