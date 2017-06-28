"""
Defines the Migrator class which can be used for migrating data into BlazingDB
"""

import asyncio
import concurrent
import logging

from blazingdb.pipeline import messages

from . import exceptions


class Migrator(object):
    """ Handles migrating data from a source into BlazingDB """

    def __init__(self, triggers, pipeline, destination, loop=None, **kwargs):  # pylint: disable=too-many-arguments
        self.logger = logging.getLogger(__name__)

        self.loop = loop
        self.processor = Processor(self._process_message, loop=loop, **kwargs)

        self.triggers = triggers
        self.pipeline = pipeline
        self.destination = destination

    async def _process_message(self, message):
        """ Processes a given message """
        message.add_packet(messages.DestinationPacket(self.destination))

        await self.pipeline.process(message)

        self.logger.info("Successfully processed message %s", message)

    async def _poll_trigger(self, trigger):
        """ Polls a trigger, placing any returned messages on the queue """
        async for message in trigger.poll():
            if not self.processor.is_running:
                break

            await self.processor.put(message)

    async def migrate(self):
        """ Begins polling triggers and processing any messages returned from them """
        if not self.processor.is_running:
            raise exceptions.StoppedException()

        poll_tasks = [self._poll_trigger(trigger) for trigger in self.triggers]
        gathered = asyncio.gather(*poll_tasks, loop=self.loop)

        try:
            await gathered
        except:
            gathered.cancel()
            raise

    async def shutdown(self):
        """ Shuts down the migrator, cancelling any currently polled triggers """
        await self.processor.shutdown()
        await self.pipeline.shutdown()

        self.logger.debug("Migrator successfully shutdown")


class Processor(object):
    """ Processes messages in a series of asyncio tasks """

    def __init__(self, callback, loop=None, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.loop = loop
        self.callback = callback
        self.is_running = True

        processor_count = kwargs.get("processor_count", 5)
        queue_length = kwargs.get("queue_length", processor_count)

        self.continue_on_error = kwargs.get("continue_on_error", False)
        self.queue = asyncio.Queue(queue_length, loop=loop)

        self.processor_tasks = self._create_processors(
            self._process_queue, processor_count, loop)

    @staticmethod
    def _create_processors(callback, count, loop):
        return [asyncio.ensure_future(callback(), loop=loop) for _ in range(count)]

    async def _process_queue(self):
        """ Polls the queue for a messages to import, before calling the callback """
        while self.is_running:
            message = await self.queue.get()

            task = asyncio.ensure_future(self.callback(message), loop=self.loop)

            try:
                await asyncio.shield(task, loop=self.loop)
            except concurrent.futures.CancelledError:
                self.logger.debug("Waiting for processing of message %s to finish", message)
                await asyncio.shield(task, loop=self.loop)
                raise
            except Exception:  # pylint: disable=broad-except
                self.logger.exception("Caught exception attempting to handle message %s", message)
                if not self.continue_on_error: break  # pylint: disable=multiple-statements
            finally:
                self.queue.task_done()

        await asyncio.shield(self.shutdown(), loop=self.loop)

    async def put(self, message):
        """ Queues a message to be processed """
        if not self.is_running:
            raise exceptions.StoppedException()

        await self.queue.put(message)

        self.logger.info("Added message %s to the import queue", message)

    async def shutdown(self):
        """ Removes all pending messages from the queue and wait for running imports to finish """
        if not self.is_running:
            await self.queue.join()
            return

        self.is_running = False

        for task in self.processor_tasks:
            task.cancel()

        while not self.queue.empty():
            while not self.queue.empty():
                self.queue.get_nowait()
                self.queue.task_done()

            await asyncio.sleep(0)

        self.logger.debug("Pending import queue cleared, waiting on queue processors")

        await self.queue.join()

        self.logger.debug("Queue processing complete")
