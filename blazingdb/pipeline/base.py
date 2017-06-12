"""
Defines the base stage class for use during data migration
"""

class BaseStage(object):
    """ Base class for all pipeline stages """

    async def _call(self, method, data):
        if hasattr(self, method):
            await getattr(self, method)(data)

    async def process(self, step, data):
        """ Processes the current stage """
        await self._call("before", data)

        yield from await step(data)

        await self._call("after", data)
