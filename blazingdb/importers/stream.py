"""
Defines the StreamImporter class which handles streaming data into BlazingDB without
writing it out to disk
"""

import logging

from . import base


class StreamImporter(base.BaseImporter):  # pylint: disable=too-few-public-methods
    """ Handles the loading of data into Blazing using a stream """

    def __init__(self, batcher, loop=None, **kwargs):
        super(StreamImporter, self).__init__(batcher, loop, **kwargs)
        self.logger = logging.getLogger(__name__)

    def _init_load(self, data):
        return {
            "connector": data["connector"],
            "table": data["dest_table"]
        }

    async def _load_batch(self, data, batch):
        await self._stream_chunk(data["connector"], batch, data["table"])

    async def _stream_chunk(self, connector, chunk, table):
        """ Streams a chunk of data into Blazing """
        rows = list(chunk)

        method = "stream '{0}'".format("".join(rows))

        self.logger.info("Streaming %s row(s) into %s", len(rows), table)
        await self._perform_request(connector, method, table)
