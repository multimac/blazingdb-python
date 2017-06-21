"""
Defines classes involved in running stages of a pipeline
"""

from collections import deque

from . import messages
from .stages import base


# pragma pylint: disable=too-few-public-methods

class System(object):
    """ Wraps an array of pipeline stages """

    def __init__(self, stages=None):
        self.stages = stages if stages is not None else []

    async def process(self, message, callback):
        """ Processes the pipeline with the given data and callback """
        message.stages = deque(self.stages)
        message.stages.append(CallbackStage(callback))

        await message.forward()


class CallbackStage(base.BaseStage):
    """ Final stage which yields the given data object """

    def __init__(self, callback):
        super(CallbackStage, self).__init__(messages.Packet)
        self.callback = callback

    async def process(self, message):
        await self.callback(message)
