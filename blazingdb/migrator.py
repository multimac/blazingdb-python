"""
Defines the Migrator class which can be used for migrating data into BlazingDB
"""

import asyncio
import concurrent
import logging

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

            self.logger.info("Added table %s to the import queue", table)

    async def _process_queue(self):
        while True:
            table = await self.queue.get()

            if table is None:
                self.queue.task_done()
                break

            self.logger.info("Popped table %s from the import queue", table)

            try:
                await self._migrate_table(table)
            except concurrent.futures.CancelledError:
                raise
            except exceptions.SkipImportException:
                pass
            except Exception:  # pylint: disable=broad-except
                self.logger.exception("Caught exception attempting to import table %s", table)
            finally:
                self.queue.task_done()

    async def migrate(self):
        """
        Migrates the given list of tables from the source into BlazingDB. If tables is not
        specified, all tables in the source are migrated
        """

        poll_tasks = [self._poll_trigger(trigger) for trigger in self.triggers]
        process_tasks = [self._process_queue() for _ in range(self.concurrent_imports)]

        gathered_poll_tasks = asyncio.gather(*poll_tasks, loop=self.loop)
        gathered_process_tasks = asyncio.gather(*process_tasks, loop=self.loop)

        try:
            await gathered_poll_tasks
        except:
            gathered_poll_tasks.cancel()
            raise
        finally:
            for _ in range(self.concurrent_imports):
                await self.queue.put(None)

            await gathered_process_tasks
