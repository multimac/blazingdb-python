"""
Defines classes involved in running stages of a pipeline
"""

import asyncio
import logging

from .stages import base


class System(object):
    """ Wraps an array of pipeline stages """

    class BlackholeStage(base.BaseStage):
        """ Empty stage to prevent messages continuing past end of pipeline """

        async def process(self, message):
            pass

        async def receive(self, message):
            pass

    def __init__(self, *stages, loop=None):
        self.logger = logging.getLogger(__name__)

        self.loop = loop
        self.stages = list(stages) + [System.BlackholeStage()]

    async def _process_message(self, message):
        await self.stages[message.stage_idx].receive(message)

    async def enqueue(self, message):
        """ Queues a given message to be processed """
        message.system = self
        asyncio.ensure_future(self._process_message(message), loop=self.loop)

    async def shutdown(self):
        pass
