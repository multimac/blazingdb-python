"""
Defines classes involved in running stages of a pipeline
"""

import logging

from blazingdb import processor

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

    def __init__(self, *stages, loop=None, **kwargs):
        self.processor = processor.Processor(self._process_message, loop=loop, **kwargs)
        self.stages = list(stages) + [System.WarningStage()]

    async def _process_message(self, message):
        message.stage_idx += 1
        message.system = self

        await self.stages[message.stage_idx].receive(message)

    async def enqueue(self, message):
        """ Queues a given message to be processed """
        await self.processor.enqueue(message)

    async def shutdown(self):
        await self.processor.shutdown()
