"""
Defines a series of miscellaneous pipeline stages, including:
 - DelayStage
 - PrefixTableStage
 - PromptInputStage
"""

import asyncio

from . import base
from .. import messages


# pragma pylint: disable=too-few-public-methods

class SemaphoreStage(base.BaseStage):
    """ Uses a semaphore to prevent access to later parts of the pipeline """

    def __init__(self, limit, loop=None):
        super(SemaphoreStage, self).__init__(messages.Packet)
        self.semaphore = asyncio.BoundedSemaphore(limit, loop=loop)

    async def process(self, message):
        async with self.semaphore:
            await message.forward()
