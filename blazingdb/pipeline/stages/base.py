"""
Defines the base stage class for use during data migration
"""

import logging
import functools

from blazingdb import exceptions


class BaseStage(object):
    """ Base class for all pipeline stages """

    def __init__(self, *message_types):
        self.types = [message_types]

    def ignore(self, message_type):
        self.types.remove(message_type)

    def listen_for(self, message_type):
        self.types.append(message_type)

    async def receive(self, message):
        """ Called when a given message is received """
        type_check = functools.partial(isinstance, message)
        if any(filter(type_check, self.types)):
            await self.process(message)
        else:
            await message.forward()

    async def _call(self, method, *args):
        if hasattr(self, method):
            await getattr(self, method)(*args)

    async def process(self, message):
        """ Processes the current stage """
        await self._call("before", message)

        try:
            await message.forward()
        except Exception as ex:
            skipped = isinstance(ex, exceptions.SkipImportException)

            try:
                await self._call("after", message, skipped, False)
            except Exception:  # pylint: disable=broad-except
                message = "Failed calling 'after' during exception handler"
                logging.getLogger(__name__).exception.exception(message)

            raise

        await self._call("after", message, False, True)
