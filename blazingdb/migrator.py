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

        self.destination = destination
        self.importer = importer
        self.pipeline = pipeline
        self.source = source
        self.triggers = triggers

        self.concurrent_imports = kwargs.get("concurrent_imports", 5)

        self.queue_length = kwargs.get("queue_length", self.concurrent_imports)
        self.queue = asyncio.Queue(self.queue_length, loop=loop)

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

    async def _poll_trigger(self, trigger):
        async for table in trigger.poll(self.source):
            await self.queue.put(table)

            self.logger.info("Added table %s to import queue", table)

    async def _process_queue(self, callback):
        while True:
            table = await self.queue.get()

            await callback(table)
            self.queue.task_done()

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

        poll_tasks = [self._poll_trigger(trigger) for trigger in self.triggers]
        process_tasks = [self._process_queue(migrate) for _ in range(self.concurrent_imports)]

        gather_task = asyncio.gather(*poll_tasks, *process_tasks, loop=self.loop)

        try:
            await gather_task
        except:
            gather_task.cancel()
            raise
