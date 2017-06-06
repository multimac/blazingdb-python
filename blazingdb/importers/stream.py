"""
Defines the StreamImporter class which handles streaming data into BlazingDB without
writing it out to disk
"""

import logging

from . import base


class StreamImporter(base.BaseImporter):  # pylint: disable=too-few-public-methods
    """ Handles the loading of data into Blazing using a stream """

    DEFAULT_CHUNK_SIZE = 1048576

    def __init__(self, loop=None, **kwargs):
        super(StreamImporter, self).__init__(loop, **kwargs)
        self.logger = logging.getLogger(__name__)

        self.chunk_size = kwargs.get("chunk_size", self.DEFAULT_CHUNK_SIZE)

    async def _stream_chunk(self, connector, chunk, table):
        """ Streams a chunk of data into Blazing """
        rows = list(chunk)

        method = "stream '{0}'".format("".join(rows))

        self.logger.info("Streaming %s row(s) into %s", len(rows), table)
        await self._perform_request(connector, method, table)

    async def load(self, data):
        """ Reads from the stream and imports the data into the table of the given name """
        connector = data["connector"]
        table = data["dest_table"]

        with self._create_stream(data) as stream_processor:
            for chunk in stream_processor.batch_bytes(self.chunk_size):
                await self._stream_chunk(connector, chunk, table)
