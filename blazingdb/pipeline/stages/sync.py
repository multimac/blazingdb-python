"""
Defines a series of miscellaneous pipeline stages, including:
 - DelayStage
 - PrefixTableStage
 - PromptInputStage
"""

import asyncio
import concurrent
import logging

from . import base
from ..import messages

# pragma pylint: disable=too-few-public-methods

class RetryStage(base.BaseStage):
    """ Adds RetryTransport to any processed message transports """

    def __init__(self, handler, blocking, loop=None):
        super(RetryStage, self).__init__(messages.Packet)
        self.transport = RetryTransport(handler, blocking, loop=loop)

    async def process(self, message):
        message.add_transport(self.transport)

        await message.forward()

class RetryTransport(messages.Transport):
    """ Custom transport which retries a message when it fails """

    def __init__(self, handler, blocking, loop=None):
        self.logger = logging.getLogger(__name__)

        self.blocking = blocking
        self.handler = handler

        self.sync_event = asyncio.Event(loop=loop)
        self.sync_event.set()

    async def _attempt(self, step, msg):
        try:
            await step(msg)
        except concurrent.futures.CancelledError:
            raise
        except Exception as ex:  # pylint: disable=broad-except
            packet_names = [type(pkt).__name__ for pkt in msg.packets]

            self.logger.warning("Caught exception attempting to forward message")
            self.logger.debug("exception: %s, packets: %s", type(ex).__name__, packet_names)

            return not await self._handle(msg, ex)

        return True

    async def _handle(self, msg, ex):
        await self.sync_event.wait()

        if self.blocking:
            self.sync_event.clear()

        try:
            return await self.handler(msg, ex)
        finally:
            self.sync_event.set()

    async def process(self, step, msg):
        while True:
            if await self._attempt(step, msg):
                break


class SemaphoreStage(base.BaseStage):
    """ Uses a semaphore to prevent access to later parts of the pipeline """

    def __init__(self, limit, loop=None):
        super(SemaphoreStage, self).__init__(messages.Message)
        self.semaphore = asyncio.BoundedSemaphore(limit, loop=loop)

    async def process(self, message):
        async with self.semaphore:
            await message.forward()
