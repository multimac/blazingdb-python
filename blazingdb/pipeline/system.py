"""
Defines classes involved in running stages of a pipeline
"""

import abc
import functools
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
            self.logger.debug("packets=%s", message.packets)

    def __init__(self, stages=None, transports=None):
        self.stages = list(stages) if stages is not None else []
        self.stages.append(System.WarningStage())

        transports = transports if transports is not None else []
        self.transport = self._build_transport(transports)

    def _build_transport(self, transports):
        func = self._forward

        for transport in transports:
            func = functools.partial(transport.process, func)

        return functools.partial(self._begin_transport, func)

    async def _begin_transport(self, step, msg):
        msg.system = self

        if msg.parent is not None:
            msg.stage_idx = msg.parent.stage_idx + 1
        else:
            msg.stage_idx = 0

        await step(msg)

    async def _forward(self, message):
        """ Forwards the given message to the next stage in the pipeline """
        await self.stages[message.stage_idx].receive(message)

    async def process(self, message):
        """ Processes the given message through the pipeline """
        await self.transport(message)


class Transport(object, metaclass=abc.ABCMeta):
    """ Base class used for all transport classes used to forward messages """

    @abc.abstractmethod
    async def process(self, step, msg):
        """ Forwards the message to the given stage """
