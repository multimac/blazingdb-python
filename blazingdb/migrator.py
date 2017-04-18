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
        import_data = {
            "connector": self.connector,
            "importer": self.importer,
            "source": self.source,
            "stream": self.source.retrieve(table),

            "columns": self.source.get_columns(table),
            "dest_table": table,
            "src_table": table
        }

        async with self.semaphore:
            for stage in self.pipeline:
                await stage.begin_import(import_data)

            await self.importer.load(self.connector, import_data)

            for stage in self.pipeline:
                await stage.end_import(import_data)

    def migrate(self, tables=None):
        """
        Migrates the given list of tables from the source into BlazingDB. If tables is not
        specified, all tables in the source are migrated
        """

        if tables is None:
            tables = self.source.get_tables()
        elif isinstance(tables, str):
            tables = [tables]

        self.logger.info("Tables to be imported: %s", ", ".join(tables))

        tasks = []
        for i, table in enumerate(tables):
            self.logger.info("Importing table %s of %s, %s", i, len(tables), table)

            tasks.append(self._migrate_table(table))

        self.loop.run_until_complete(
            asyncio.gather(*tasks, loop=self.loop)
        )
