"""
Defines the base class for all triggers
"""

import abc

class BaseTrigger(object):  # pylint: disable=too-few-public-methods
    """ Base class for all triggers """

    @abc.abstractmethod
    async def poll(self):
        """ Retrieves an async generator for polling this trigger """
