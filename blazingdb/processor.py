"""
Defines the Migrator class which can be used for migrating data into BlazingDB
"""

import asyncio
import concurrent
import logging

import enum

from . import exceptions


class State(enum.Enum):
    """ Representst the state of a Processor """

    Running = enum.auto()
    Stopping = enum.auto()
    Stopped = enum.auto()

class Processor(object):
    """ Processes messages in a series of asyncio tasks """

    def __init__(self, callback, loop=None, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.callback = callback
        self.state = State.Running

        processor_count = kwargs.get("processor_count", 5)
        queue_length = kwargs.get("queue_length", processor_count)

        self.continue_on_error = kwargs.get("continue_on_error", False)
        self.processors = self._create_processors(self._process_queue, processor_count, loop)
        self.queue = asyncio.Queue(queue_length, loop=loop)

    @staticmethod
    def _create_processors(callback, count, loop):
        return [asyncio.ensure_future(callback(), loop=loop) for _ in range(count)]

    async def _process_queue(self):
        """ Polls the queue for a messages to import, before calling the callback """
        while True:
            args = await self.queue.get()

            try:
                await self.callback(*args)
            except concurrent.futures.CancelledError:
                self.logger.warning("Processor task cancelled during callback")
                raise
            except Exception:  # pylint: disable=broad-except
                self.logger.exception("Caught exception attempting to callback with %s", args)
                if not self.continue_on_error: break  # pylint: disable=multiple-statements
            finally:
                self.queue.task_done()

        await asyncio.shield(self.shutdown())

    async def enqueue(self, *args):
        """ Queues a message to be processed """
        if self.state is not State.Running:
            raise exceptions.StoppedException()

        await self.queue.put(args)

    async def clear(self):
        """ Clears all pending messages from the queue """
        while not self.queue.empty():
            self.queue.get_nowait()
            self.queue.task_done()

            await asyncio.sleep(0)

        self.logger.debug("Pending import queue cleared")

    async def shutdown(self):
        """ Removes all pending messages from the queue and wait for running imports to finish """
        if self.state is not State.Running:
            await self.queue.join()
            return

        self.state = State.Stopping
        self.logger.debug("Waiting on processor tasks to complete")

        await self.clear()

        try:
            await self.queue.join()
        except concurrent.futures.CancelledError:
            self.logger.debug("Forcefully cancelling running processors")

        for task in self.processors:
            task.cancel()

        self.state = State.Stopped
        self.logger.debug("Processor stopped")
