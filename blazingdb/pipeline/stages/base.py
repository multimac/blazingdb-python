"""
Defines the base stage class for use during data migration
"""

import abc
import logging

import enum

from blazingdb import exceptions


class When(enum.Flag):
    """ Defines the stages at which a custom query can be executed """
    before = enum.auto()
    after = enum.auto()


class BaseStage(object, metaclass=abc.ABCMeta):
    """ Base class for all pipeline stages """

    def __init__(self, *message_types):
        self.types = set(message_types)

    def ignore(self, message_type):
        self.types.remove(message_type)

    def listen_for(self, message_type):
        self.types.add(message_type)

    @abc.abstractmethod
    async def process(self, message):
        pass

    async def receive(self, message):
        """ Called when a given message is received """
        if isinstance(message, self.types):
            await self.process(message)
        else:
            await message.forward()


class PipelineStage(BaseStage):
    """ General base class for pipeline stages """

    async def before(self, message):
        pass

    async def after(self, message, skipped, success):
        pass

    async def process(self, message):
        """ Processes the current stage """
        await self.before(message)

        try:
            await message.forward()
        except Exception as ex:
            skipped = isinstance(ex, exceptions.SkipImportException)

            try:
                await self.after(message, skipped, False)
            except Exception:  # pylint: disable=broad-except
                message = "Failed calling 'after' during exception handler"
                logging.getLogger(__name__).exception.exception(message)

            raise

        await self.after(message, False, True)
