"""
Defines the Migrator class which can be used for migrating data into BlazingDB
"""

import asyncio
import logging


class Migrator(object):  # pylint: disable=too-few-public-methods
    """ Handles migrating data from a source into BlazingDB """

    def __init__(self, connector, source, pipeline, importer, loop=None, import_limit=5):  # pylint: disable=too-many-arguments
        self.logger = logging.getLogger(__name__)

        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.semaphore = asyncio.BoundedSemaphore(import_limit)

        self.connector = connector
        self.importer = importer
        self.pipeline = pipeline
        self.source = source

    async def _migrate_table(self, table):
        """ Imports an individual table into BlazingDB """
        async with self.semaphore:
            self.logger.info("Importing table %s...", table)

            import_data = {
                "connector": self.connector,
                "importer": self.importer,
                "source": self.source,
                "stream": self.source.retrieve(table),

                "columns": self.source.get_columns(table),
                "dest_table": table,
                "src_table": table
            }

            for stage in self.pipeline:
                await stage.begin_import(import_data)

            await self.importer.load(self.connector, import_data)

            for stage in self.pipeline:
                await stage.end_import(import_data)

            self.logger.info("Successfully imported table %s", table)

    async def _safe_migrate_table(self, table):
        """ Imports an individual table into BlazingDB, but handles exceptions if they occur """
        try:
            self._migrate_table(table)
        except Exception:  # pylint: disable=broad-except
            self.logger.exception("Failed to import table %s", table)

    def migrate(self, tables=None, **kwargs):
        """
        Migrates the given list of tables from the source into BlazingDB. If tables is not
        specified, all tables in the source are migrated
        """

        if kwargs.get("continue_on_error", False):
            migrate = self._safe_migrate_table
        else:
            migrate = self._migrate_table

        if tables is None:
            tables = self.source.get_tables()
        elif isinstance(tables, str):
            tables = [tables]

        self.logger.info("Tables to be imported: %s", ", ".join(tables))

        tasks = []
        for table in tables:
            tasks.append(migrate(table))

        self.loop.run_until_complete(
            asyncio.gather(*tasks, loop=self.loop)
        )
