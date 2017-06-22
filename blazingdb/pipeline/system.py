"""
Defines classes involved in running stages of a pipeline
"""

import collections
import logging

from . import messages
from .stages import base


# pragma pylint: disable=too-few-public-methods

class System(object):
    """ Wraps an array of pipeline stages """

    def __init__(self, stages=None):
        self.stages = stages if stages is not None else []

    async def process(self, message):
        """ Processes the given message through the pipeline """
        message.stages = collections.deque(self.stages)
        message.stages.append(WarningStage())

        await message.forward()


class WarningStage(base.BaseStage):
    """ Warns that a message has reached this stage in the pipeline """
    def __init__(self):
        super(WarningStage, self).__init__(messages.Packet)
        self.logger = logging.getLogger(__name__)

    async def process(self, message):
        self.logger.warning("Message reached the end of the pipeline without being consumed")
        self.logger.debug("packets=%s", message.packets)
