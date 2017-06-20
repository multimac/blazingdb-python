"""
Defines the StreamImporter class which handles streaming data into BlazingDB without
writing it out to disk
"""

import logging

from blazingdb.pipeline import messages
from . import base


class StreamImporter(base.BaseImporter):  # pylint: disable=too-few-public-methods
    """ Handles the loading of data into Blazing using a stream """

    def __init__(self, loop=None, **kwargs):
        super(StreamImporter, self).__init__(loop, **kwargs)
        self.logger = logging.getLogger(__name__)

    async def load(self, message):
        """ Streams a chunk of data into Blazing """
        import_pkt = message.get_packet(messages.ImportTablePacket)
        format_pkt = message.get_packet(messages.DataFormatPacket)

        destination = import_pkt.destination
        table = import_pkt.table
        fmt = format_pkt.fmt

        for load_pkt in message.get_packets(messages.DataLoadPacket):
            method = "stream '{0}'".format("".join(load_pkt.data))

            self.logger.info("Streaming %s row(s) into %s", len(load_pkt.data), table)
            await self._perform_request(destination, method, fmt, table)
