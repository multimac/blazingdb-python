"""
Defines a trigger which simply retrieves / returns all tables from the given source
"""

import logging

from blazingdb.pipeline import messages

from . import base


class SourceTrigger(base.BaseTrigger):  # pylint: disable=too-few-public-methods
    """ A simple trigger which returns all tables from a source """

    def __init__(self, source):
        self.logger = logging.getLogger(__name__)
        self.source = source

    async def close(self):
        await self.source.close()

    async def poll(self):
        tables = await self.source.get_tables()

        self.logger.info("Tables to be imported: %s", ", ".join(tables))

        for table in tables:
            yield messages.Message(messages.ImportTablePacket(self.source, table))
