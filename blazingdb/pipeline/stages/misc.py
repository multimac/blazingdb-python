"""
Defines a series of miscellaneous pipeline stages, including:
 - DelayStage
 - PrefixTableStage
 - PromptInputStage
"""

import asyncio
import fnmatch
import logging

from blazingdb import exceptions

from . import base, custom
from .. import messages


class DelayStage(custom.CustomActionStage):
    """ Pauses the pipeline before / after importing data """

    def __init__(self, delay, **kwargs):
        super(DelayStage, self).__init__(self._delay, **kwargs)
        self.prompt = kwargs.get("prompt", "Waiting for input...")
        self.delay = delay

    async def _delay(self, data):  # pylint: disable=unused-argument
        await asyncio.sleep(self.delay)


class PromptInputStage(custom.CustomActionStage):
    """ Prompts for user input to continue before / after importing data """

    def __init__(self, **kwargs):
        super(PromptInputStage, self).__init__(self._prompt, **kwargs)
        self.prompt = kwargs.get("prompt", "Waiting for input...")

    async def _prompt(self, data):  # pylint: disable=unused-argument
        input(self.prompt)


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


class SkipTableStage(base.BaseStage):
    """ Skips tables based on a given set of inclusions and exclusions """

    def __init__(self, included=None, excluded=None):
        super(SkipTableStage, self).__init__(messages.ImportTablePacket)

        self.included = included
        self.excluded = excluded

    def _filter_table(self, table):
        if self.excluded is not None:
            for pattern in self.excluded:
                if fnmatch.fnmatch(table, pattern):
                    return True

        if self.included is None:
            return False

        for pattern in self.included:
            if fnmatch.fnmatch(table, pattern):
                return False

        return True

    async def process(self, message):
        """ Only calls message.forward if the message isn't filtered """
        import_pkt = message.get_packet(messages.ImportTablePacket)

        if not self._filter_table(import_pkt.table):
            await message.forward()


class SkipUntilStage(base.BaseStage):
    """ Skips tables based on a given set of inclusions and exclusions """

    def __init__(self, pattern, include_matched=True):
        super(SkipUntilStage, self).__init__(messages.ImportTablePacket)

        self.include_matched = include_matched
        self.pattern = pattern
        self.matched = False

    async def process(self, message):
        """ Only calls message.forward if the message isn't filtered """
        if self.matched:
            await message.forward()
            return

        import_pkt = message.get_packet(messages.ImportTablePacket)

        if fnmatch.fnmatch(import_pkt.table, self.pattern):
            self.matched = True

            if self.include_matched:
                await message.forward()
