"""
Defines the base class for all triggers
"""

import abc

from blazingdb.pipeline import messages, packets


# pylint: disable=too-few-public-methods

class BaseTrigger(object):
    """ Base class for all triggers """

    @abc.abstractmethod
    async def poll(self):
        """ Retrieves an async generator for polling this trigger """


class TableTrigger(BaseTrigger):
    """ A utility class for generating messages """

    def __init__(self, source):
        self.source = source

    @abc.abstractmethod
    async def _poll(self):
        pass

    async def poll(self):
        async for table in self._poll():
            packet = packets.ImportTablePacket(self.source, table)

            yield messages.Message(packet)
