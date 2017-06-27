"""
Defines classes involved in running stages of a pipeline
"""

import asyncio
import logging

from . import messages
from .stages import base


# pragma pylint: disable=too-few-public-methods

class System(object):
    """ Wraps an array of pipeline stages """

    class WarningStage(base.BaseStage):
        """ Warns that a message has reached this stage in the pipeline """
        def __init__(self):
            super(System.WarningStage, self).__init__(messages.Packet)
            self.logger = logging.getLogger(__name__)

        async def process(self, message):
            self.logger.warning("Message reached the end of the pipeline without being consumed")
            self.logger.debug("%r", message)

    def __init__(self, stages=None, loop=None):
        self.loop = loop

        self.stages = list(stages) if stages is not None else []
        self.stages.append(System.WarningStage())

    async def process(self, message):
        """ Processes the given message through the pipeline """
        message.system = self

        if message.parent is not None:
            message.stage_idx = message.parent.stage_idx + 1
        else:
            message.stage_idx = 0

        await self.stages[message.stage_idx].receive(message)

    async def shutdown(self):
        tasks = map(lambda stg: stg.shutdown(), self.stages)
        await asyncio.gather(*tasks, loop=self.loop)
