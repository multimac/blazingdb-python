"""
Defines the Migrator class which can be used for migrating data into BlazingDB
"""

import asyncio
import logging

from functools import partial

from . import exceptions


class Migrator(object):  # pylint: disable=too-few-public-methods,too-many-instance-attributes

    """ Handles migrating data from a source into BlazingDB """

    def __init__(self, triggers, source, pipeline, importer, destination, loop=None):  # pylint: disable=too-many-arguments
        self.logger = logging.getLogger(__name__)

        self.loop = loop

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
        import_data = {
            "source": self.source,
            "src_table": table,
            "dest_table": table,
            "destination": self.destination
        }

        async for pipeline_data in self.pipeline.process(import_data):
            await self.importer.load(pipeline_data)

        self.logger.info("Successfully imported table %s", table)

    async def _poll_trigger(self, trigger, callback):
        tasks = []
        async for table in trigger.poll(self.source):
            task = asyncio.ensure_future(callback(table), loop=self.loop)
            tasks.append(task)

        await asyncio.gather(*tasks, loop=self.loop)

    async def _retrying_import(self, handler, table):
        while True:
            try:
                await self._migrate_table(table)
            except exceptions.BlazingException as ex:
                if isinstance(ex, exceptions.SkipImportException):
                    return

                self.logger.error("Failed to import table %s (%s): %s", table, type(ex), ex)

                if not await handler(table, ex):
                    break

    async def migrate(self, **kwargs):
        """
        Migrates the given list of tables from the source into BlazingDB. If tables is not
        specified, all tables in the source are migrated
        """

        def raise_exception(table, ex):  # pylint: disable=unused-argument
            raise exceptions.MigrateException() from ex

        if kwargs.get("retry_handler", None) is not None:
            migrate = partial(self._retrying_import, kwargs.get("retry_handler"))
        elif kwargs.get("continue_on_error", False):
            migrate = partial(self._retrying_import, lambda table, ex: False)
        else:
            migrate = partial(self._retrying_import, raise_exception)

        tasks = [self._poll_trigger(trigger, migrate) for trigger in self.triggers]
        gather_task = asyncio.gather(*tasks, loop=self.loop)

        try:
            await gather_task
        except Exception:  # pylint: disable=broad-except
            self.logger.exception("Failed to migrate all tables")

            gather_task.cancel()
