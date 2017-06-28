"""
Defines the base class for all triggers
"""

import abc

from blazingdb.pipeline import messages

class BaseTrigger(object):  # pylint: disable=too-few-public-methods
    """ Base class for all triggers """

    async def close(self):
        """ Closes any resources used by the given trigger """

    @abc.abstractmethod
    async def poll(self):
        """ Retrieves an async generator for polling this trigger """


class TableTrigger(BaseTrigger):
    """ A utility class for generating ImportTablePacket messages """

    def __init__(self, source):
        self.source = source

    async def close(self):
        await self.close()

    @abc.abstractmethod
    async def _poll(self):
        pass

    async def poll(self):
        async for table in self._poll():
            yield messages.Message(messages.ImportTablePacket(self.source, table))
