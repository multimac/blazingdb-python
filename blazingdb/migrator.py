"""
Defines the Migrator class which can be used for migrating data into BlazingDB
"""

import asyncio
import logging

from functools import partial

from . import exceptions


class Migrator(object):  # pylint: disable=too-few-public-methods,too-many-instance-attributes

    """ Handles migrating data from a source into BlazingDB """

    def __init__(self, triggers, source, pipeline, importer, destination, loop=None, **kwargs):  # pylint: disable=too-many-arguments
        self.logger = logging.getLogger(__name__)

        self.loop = loop
        self.semaphore = asyncio.BoundedSemaphore(
            kwargs.get("import_limit", 5), loop=loop
        )

        self.destination = destination
        self.importer = importer
        self.pipeline = pipeline
        self.source = source
        self.triggers = triggers

    def close(self):
        self.destination.close()
        self.source.close()

    async def _migrate_table(self, table):
        """ Imports an individual table into BlazingDB """
        self.logger.info("Importing table %s...", table)

        import_data = {
            "source": self.source,
            "src_table": table,
            "dest_table": table,
            "destination": self.destination
        }

        async for pipeline_data in self.pipeline.process(import_data):
            await self.importer.load(pipeline_data)

        self.logger.info("Successfully imported table %s", table)

    async def _safe_migrate_table(self, retry_handler, trigger):
        """ Imports an individual table into BlazingDB, but handles exceptions if they occur """
        async for table in trigger.poll(self.source):
            async with self.semaphore:
                try:
                    await self._migrate_table(table)
                    continue
                except exceptions.BlazingException as ex:
                    if isinstance(ex, exceptions.SkipImportException):
                        continue

                    self.logger.error("Failed to import table %s (%s): %s", table, type(ex), ex)

                    if not await retry_handler(table, ex):
                        continue

    async def migrate(self, **kwargs):
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

        tasks = [migrate(trigger) for trigger in self.triggers]
        gather_task = asyncio.gather(*tasks, loop=self.loop)

        try:
            await gather_task
        except Exception:  # pylint: disable=broad-except
            self.logger.exception("Failed to migrate all tables")

            gather_task.cancel()
