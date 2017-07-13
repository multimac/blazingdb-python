"""
Defines the base stage class for use during data migration
"""

import abc

import enum


class When(enum.Flag):
    """ Defines the stages at which a custom query can be executed """
    before = enum.auto()
    after = enum.auto()

    always = before | after


class BaseStage(object, metaclass=abc.ABCMeta):
    """ Base class for all pipeline stages """

    def __init__(self, *packet_types):
        self.types = set(packet_types)

    @abc.abstractmethod
    async def process(self, message):
        pass

    async def receive(self, message):
        """ Called when a given message is received """
        try:
            if any(message.get_packets(*self.types)):
                await self.process(message)
            else:
                await message.forward()
        finally:
            message.complete()

    async def shutdown(self):
        pass


class PipelineStage(BaseStage):
    """ General base class for pipeline stages """

    async def before(self, message):
        pass

    async def after(self, message):
        pass

    async def process(self, message):
        """ Processes the current stage """
        await self.before(message)
        await (await message.forward(track_children=True))

        await self.after(message)
