"""
Defines the StreamImporter class which handles streaming data into BlazingDB without
writing it out to disk
"""

import logging

from . import base, processor


class StreamImporter(base.BaseImporter):  # pylint: disable=too-few-public-methods
    """ Handles the loading of data into Blazing using a stream """

    DEFAULT_CHUNK_SIZE = 1048576

    def __init__(self, **kwargs):
        super(StreamImporter, self).__init__(**kwargs)
        self.logger = logging.getLogger(__name__)

        self.processor_args = kwargs
        self.chunk_size = kwargs.get("chunk_size", self.DEFAULT_CHUNK_SIZE)

    async def _stream_chunk(self, connector, data, table):
        """ Streams a chunk of data into Blazing """
        method = "stream '{0}'".format("".join(data))

        self.logger.info("Streaming %s row(s) into %s", len(data), table)
        await self._perform_request(connector, method, table)

    async def load(self, connector, data):
        """ Reads from the stream and imports the data into the table of the given name """
        stream_processor = processor.StreamProcessor(data["stream"], **self.processor_args)

        while True:
            try:
                chunk_data = stream_processor.read_bytes(self.chunk_size)
            except StopIteration:
                break

            await self._stream_chunk(connector, chunk_data, data["dest_table"])
