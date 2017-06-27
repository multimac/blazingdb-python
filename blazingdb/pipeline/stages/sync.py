"""
Defines a series of miscellaneous pipeline stages, including:
 - DelayStage
 - PrefixTableStage
 - PromptInputStage
"""

import asyncio
import logging

from blazingdb import exceptions

from . import base
from .. import messages


# pragma pylint: disable=too-few-public-methods

class RetryStage(base.BaseStage):
    """ Retries a given message if an exception bubbles up when forwarding it """

    def __init__(self, handler, max_retries=None):
        super(RetryStage, self).__init__(messages.Packet)
        self.logger = logging.getLogger(__name__)

        self.handler = handler
        self.max_retries = max_retries

    async def _attempt(self, message):
        try:
            await message.forward()
        except Exception as ex:  # pylint: disable=broad-except
            self.logger.warning("Caught exception %s while processing message %s", ex, message)
            return await self._handle(message, ex)

        return False

    async def _handle(self, message, ex):
        pass

    async def process(self, message):
        attempts = 0
        while self.max_retries is None or attempts < self.max_retries:
            if not await self._attempt(message):
                break

            attempts += 1

        raise exceptions.RetryException()


class SemaphoreStage(base.BaseStage):
    """ Uses a semaphore to prevent access to later parts of the pipeline """

    def __init__(self, limit, loop=None):
        super(SemaphoreStage, self).__init__(messages.Packet)
        self.semaphore = asyncio.BoundedSemaphore(limit, loop=loop)

    async def process(self, message):
        async with self.semaphore:
            await message.forward()
