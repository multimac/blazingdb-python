"""
Defines a trigger which simply retrieves / returns all tables from the given source
"""

import logging

from . import base


class SourceTrigger(base.BaseTrigger):  # pylint: disable=too-few-public-methods
    """ A simple trigger which returns all tables from a source """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    async def poll(self, source):
        tables = await source.get_tables()

        self.logger.info("Tables to be imported: %s", ", ".join(tables))

        return tables
