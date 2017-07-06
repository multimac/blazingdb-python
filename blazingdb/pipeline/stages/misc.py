"""
Defines a series of miscellaneous pipeline stages, including:
 - DelayStage
 - PrefixTableStage
 - PromptInputStage
"""

import asyncio
import collections
import fnmatch
import logging

from blazingdb import exceptions

from . import base, custom
from .. import packets


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

    def __init__(self, handler, max_attempts=None):
        super(RetryStage, self).__init__(packets.Packet)
        self.logger = logging.getLogger(__name__)

        self.handler = handler
        self.max_attempts = max_attempts

    async def _attempt(self, message):
        try:
            await message.forward()
        except Exception as ex:  # pylint: disable=broad-except
            self.logger.warning("Caught exception %s while processing message %s", ex, message)
            return await self.handler(message, ex)

        return False

    async def process(self, message):
        attempts = 0
        while self.max_attempts is None or attempts < self.max_attempts:
            if not await self._attempt(message):
                return

            attempts += 1

        raise exceptions.RetryException()


class SemaphoreStage(base.BaseStage):
    """ Uses a semaphore to prevent access to later parts of the pipeline """

    def __init__(self, limit, loop=None):
        super(SemaphoreStage, self).__init__(packets.Packet)
        self.semaphore = asyncio.BoundedSemaphore(limit, loop=loop)

    async def process(self, message):
        async with self.semaphore:
            await message.forward()


class SingleFileStage(base.BaseStage):
    """ Collects messages generated from an initial message and processes them one at a time """

    Processor = collections.namedtuple("Processor", ["task", "queue"])

    def __init__(self, loop=None):
        super(SingleFileStage, self).__init__(packets.Packet)

        self.loop = loop
        self.processors = dict()

    def _get_processor(self, msg_id):
        if msg_id not in self.processors:
            processor = SingleFileStage.Processor(
                queue=asyncio.Queue(loop=self.loop),
                task=asyncio.ensure_future(self._process_queue(msg_id), loop=self.loop))

            self.processors[msg_id] = processor

        return self.processors[msg_id]

    async def _process_queue(self, msg_id):
        queue = self.processors[msg_id].queue

        while not queue.empty():
            message = await queue.get()
            handle = await message.forward()

            await handle

            queue.task_done()

        self.processors.pop(msg_id)

    async def process(self, message):
        msg_id = message.msg_id
        processor = self._get_processor(msg_id)

        await processor.queue.put(message)
        await processor.task


class SkipTableStage(base.BaseStage):
    """ Skips tables based on a given set of inclusions and exclusions """

    def __init__(self, included=None, excluded=None):
        super(SkipTableStage, self).__init__(packets.ImportTablePacket)

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
        import_pkt = message.get_packet(packets.ImportTablePacket)

        if not self._filter_table(import_pkt.table):
            await message.forward()


class SkipUntilStage(base.BaseStage):
    """ Skips tables based on a given set of inclusions and exclusions """

    def __init__(self, pattern, include_matched=True):
        super(SkipUntilStage, self).__init__(packets.ImportTablePacket)

        self.include_matched = include_matched
        self.pattern = pattern
        self.matched = False

    async def process(self, message):
        """ Only calls message.forward if the message isn't filtered """
        if self.matched:
            await message.forward()
            return

        import_pkt = message.get_packet(packets.ImportTablePacket)

        if fnmatch.fnmatch(import_pkt.table, self.pattern):
            self.matched = True

            if self.include_matched:
                await message.forward()
