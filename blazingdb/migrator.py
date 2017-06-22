"""
Defines the Migrator class which can be used for migrating data into BlazingDB
"""

import asyncio
import concurrent
import logging

from . import exceptions
from .pipeline import messages


class Migrator(object):  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """ Handles migrating data from a source into BlazingDB """

    def __init__(self, triggers, source, pipeline, destination, loop=None, **kwargs):  # pylint: disable=too-many-arguments
        self.logger = logging.getLogger(__name__)

        self.loop = loop

        self.processor = Processor(self._migrate_table, loop=loop, **kwargs)

        self.triggers = triggers
        self.source = source
        self.pipeline = pipeline
        self.destination = destination

    def close(self):
        self.destination.close()
        self.source.close()

    async def _migrate_table(self, table):
        """ Imports an individual table into BlazingDB """
        import_packet = messages.ImportTablePacket(self.destination, self.source, table)
        import_message = messages.Message(import_packet)

        await self.pipeline.process(import_message)

        self.logger.info("Successfully imported table %s", table)

    async def _poll_trigger(self, trigger):
        """ Polls a trigger, placing any returned tables on the queue """
        async for table in trigger.poll(self.source):
            await self.processor.put(table)

    async def migrate(self):
        """ Begins polling triggers and importing any tables returned from them """
        if not self.processor.is_running:
            raise exceptions.StoppedException()

        poll_tasks = [self._poll_trigger(trigger) for trigger in self.triggers]
        gathered_tasks = asyncio.gather(*poll_tasks, loop=self.loop)

        try:
            await gathered_tasks
        except:
            gathered_tasks.cancel()
            raise

    async def shutdown(self):
        """ Shuts down the migrator, cancelling any currently polled triggers """
        await self.processor.shutdown()


class Processor(object):
    """ Processes the importing of tables """

    def __init__(self, callback, loop=None, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.loop = loop
        self.callback = callback
        self.is_running = True

        processor_count = kwargs.get("processor_count", 5)
        queue_length = kwargs.get("queue_length", processor_count)

        self.continue_on_error = kwargs.get("continue_on_error", False)
        self.queue = asyncio.Queue(queue_length, loop=loop)

        self.processor_task = asyncio.ensure_future(self._run(processor_count), loop=loop)

    async def _process_queue(self):
        """ Polls the queue for a table to import, before calling _migrate_table """
        while self.is_running:
            table = await self.queue.get()

            self.logger.info("Popped table %s from the import queue", table)

            try:
                await asyncio.shield(self.callback(table), loop=self.loop)
            except concurrent.futures.CancelledError:
                raise
            except Exception:  # pylint: disable=broad-except
                self.logger.exception("Caught exception attempting to import table %s", table)
                if not self.continue_on_error: break  # pylint: disable=multiple-statements
            finally:
                self.queue.task_done()

    async def _run(self, count):
        tasks = [asyncio.ensure_future(self._process_queue(), loop=self.loop) for _ in range(count)]

        await asyncio.wait(tasks, loop=self.loop, return_when=asyncio.FIRST_COMPLETED)
        self.logger.debug("Processor task has returned, shutting down processor")

        await self.shutdown()

    async def put(self, table):
        """ Queues a table to be processed """
        if not self.is_running:
            raise exceptions.StoppedException()

        await self.queue.put(table)

        self.logger.info("Added table %s to the import queue", table)

    async def shutdown(self):
        """ Removes all pending tables from the queue and wait for running imports to finish """
        if not self.is_running:
            await self.queue.join()
            return

        self.is_running = False

        self.logger.debug("Emptying processor queue")
        while not self.queue.empty():
            await self.queue.get()
            self.queue.task_done()

        self.processor_task.cancel()
