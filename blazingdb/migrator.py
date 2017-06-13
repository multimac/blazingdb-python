"""
Defines the Migrator class which can be used for migrating data into BlazingDB
"""

import asyncio
import concurrent
import fnmatch
import logging

from functools import partial

from . import exceptions


class Migrator(object):  # pylint: disable=too-few-public-methods
    """ Handles migrating data from a source into BlazingDB """

    def __init__(self, connector, source, pipeline, importer, loop=None, **kwargs):  # pylint: disable=too-many-arguments
        self.logger = logging.getLogger(__name__)

        self.loop = loop
        self.semaphore = asyncio.BoundedSemaphore(
            kwargs.get("import_limit", 5), loop=loop
        )

        self.connector = connector
        self.importer = importer
        self.pipeline = pipeline
        self.source = source

    def _retrieve_tables(self, included_tables, excluded_tables):
        source_tables = self.source.get_tables()
        migrate_tables = set()

        if included_tables is not None:
            for pattern in included_tables:
                matches = fnmatch.filter(source_tables, pattern)
                migrate_tables.update(matches)
        else:
            migrate_tables.update(source_tables)

        if excluded_tables is not None:
            for pattern in excluded_tables:
                matches = fnmatch.filter(migrate_tables, pattern)
                migrate_tables.difference_update(matches)

        return migrate_tables

    async def _migrate_table(self, table):
        """ Imports an individual table into BlazingDB """
        self.logger.info("Importing table %s...", table)

        import_data = {
            "connector": self.connector,
            "importer": self.importer,
            "source": self.source,
            "dest_table": table,
            "src_table": table
        }

        async for pipeline_data in self.pipeline.process(import_data):
            await pipeline_data["importer"].load(pipeline_data)

        self.logger.info("Successfully imported table %s", table)

    async def _safe_migrate_table(self, retry_handler, table):
        """ Imports an individual table into BlazingDB, but handles exceptions if they occur """

        while True:
            async with self.semaphore:
                try:
                    await self._migrate_table(table)
                    return
                except Exception as ex:  # pylint: disable=broad-except
                    if isinstance(ex, exceptions.SkipImportException):
                        return
                    elif isinstance(ex, concurrent.futures.CancelledError):
                        raise

                    self.logger.error("Failed to import table %s (%s): %s", table, type(ex), ex)

                    if not await retry_handler(table, ex):
                        return

    async def migrate(self, included_tables=None, excluded_tables=None, **kwargs):
        """
        Migrates the given list of tables from the source into BlazingDB. If tables is not
        specified, all tables in the source are migrated
        """

        def raise_exception(table, ex):  # pylint: disable=unused-argument
            raise exceptions.MigrateException() from ex

        if kwargs.get("retry_handler", None) is not None:
            migrate = partial(self._safe_migrate_table, kwargs.get("retry_handler"))
        elif kwargs.get("continue_on_error", False):
            migrate = partial(self._safe_migrate_table, lambda table, ex: False)
        else:
            migrate = partial(self._safe_migrate_table, raise_exception)

        tables = self._retrieve_tables(included_tables, excluded_tables)

        self.logger.info("Tables to be imported: %s", ", ".join(tables))

        tasks = [migrate(table) for table in tables]
        gather_task = asyncio.gather(*tasks, loop=self.loop)

        try:
            await gather_task
        except Exception:  # pylint: disable=broad-except
            self.logger.exception("Failed to migrate all tables")

            gather_task.cancel()
            await asyncio.sleep(0)
