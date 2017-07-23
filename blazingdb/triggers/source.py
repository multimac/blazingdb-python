"""
Defines a trigger which simply retrieves / returns all tables from the given source
"""

import logging

from . import base


# pylint: disable=too-few-public-methods

class SourceTrigger(base.TableTrigger):
    """ A simple trigger which returns all tables from a source """

    def __init__(self, source):
        super(SourceTrigger, self).__init__(source)
        self.logger = logging.getLogger(__name__)

    async def _poll(self):
        tables = await self.source.get_tables()

        self.logger.info("Tables to be imported: %s", ", ".join(tables))

        blah = False
        for table in tables:
            if table == "dim_supervisor_scd":
                blah = True

            if not blah and table not in ["absence", "application", "application_event"]:
                continue

            yield table
