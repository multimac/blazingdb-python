"""
Defines classes involved in running stages of a pipeline
"""

import asyncio
import logging

from blazingdb import processor

from . import packets
from .stages import base


class System(object):
    """ Wraps an array of pipeline stages """

    class BlackholeStage(base.BaseStage):
        """ Empty stage to prevent messages continuing past end of pipeline """

        async def process(self, message):
            pass

        async def receive(self, message):
            pass

    def __init__(self, *stages, loop=None, processor_count=5):
        self.logger = logging.getLogger(__name__)
        self.stages = list(stages) + [System.BlackholeStage()]

        self.processor = processor.Processor(
            self._process_message, loop=loop, enqueue_while_stopping=True,
            processor_count=processor_count, queue_length=0)

    async def _process_message(self, message):
        await self.stages[message.stage_idx].receive(message)

    async def enqueue(self, message):
        """ Queues a given message to be processed """
        message.system = self

        await self.processor.enqueue(message)

    async def process(self, message):
        await self.enqueue(message)
        await message.handle

    async def shutdown(self):
        await self.processor.shutdown()
