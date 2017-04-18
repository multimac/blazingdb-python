"""
Defines the base stage class for use during data migration
"""

class BaseStage(object):
    """ Base class for all pipeline stages """

    async def begin_import(self, data):
        """ Called before data begins being piped through the pipeline """
        pass

    async def end_import(self, data):
        """ Called after before data begins being piped through the pipeline """
        pass
