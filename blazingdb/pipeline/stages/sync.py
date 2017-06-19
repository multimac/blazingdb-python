"""
Defines a series of miscellaneous pipeline stages, including:
 - DelayStage
 - PrefixTableStage
 - PromptInputStage
"""

import asyncio
import concurrent
import logging

from blazingdb import exceptions
from . import base

# pragma pylint: disable=too-few-public-methods

class RetryStage(base.BaseStage):
    """ Handles retrying an import when it fails """

    def __init__(self, handler, loop=None, **kwargs):
        super(RetryStage, self).__init__()
        self.logger = logging.getLogger(__name__)

        self.handler = handler

        self.blocking = kwargs.get("blocking", False)
        self.sync_event = asyncio.Event(loop=loop)
        self.sync_event.set()

    async def _handle(self, data, ex):
        first = self.sync_event.is_set()
        await self.sync_event.wait()

        if self.blocking:
            self.sync_event.clear()

        try:
            return await self.handler(data, ex, first)
        finally:
            self.sync_event.set()

    async def _attempt(self, step, data):
        try:
            await self.sync_event.wait()

            async for item in step():
                yield item
        except (concurrent.futures.CancelledError, exceptions.SkipImportException):
            raise
        except Exception as ex:  # pylint: disable=broad-except
            self.logger.warning("Caught exception attempting to import table")

            return not await self._handle(data, ex)

        return True

    async def process(self, step, data):
        while True:
            if self._attempt(step, data):
                break


class SemaphoreStage(base.BaseStage):
    """ Uses a semaphore to prevent access to later parts of the pipeline """

    def __init__(self, limit, loop=None):
        super(SemaphoreStage, self).__init__()
        self.semaphore = asyncio.BoundedSemaphore(limit, loop=loop)

    async def process(self, step, data):
        async with self.semaphore:
            async for item in step():
                yield item
