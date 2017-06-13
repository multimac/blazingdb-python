"""
Defines the StreamImporter class which handles streaming data into BlazingDB without
writing it out to disk
"""

import logging

from . import base


class StreamImporter(base.BaseImporter):  # pylint: disable=too-few-public-methods
    """ Handles the loading of data into Blazing using a stream """

    def __init__(self, loop=None, **kwargs):
        super(StreamImporter, self).__init__(loop, **kwargs)
        self.logger = logging.getLogger(__name__)

    async def load(self, data):
        """ Streams a chunk of data into Blazing """
        connector = data["connector"]
        table = data["table"]
        fmt = data["fmt"]

        rows = [item async for item in data["stream"]]

        if not rows:
            self.logger.info("Skipping %s as no rows were retrieved", table)
            return

        method = "stream '{0}'".format("".join(rows))

        self.logger.info("Streaming %s row(s) into %s", len(rows), table)
        await self._perform_request(connector, method, fmt, table)
