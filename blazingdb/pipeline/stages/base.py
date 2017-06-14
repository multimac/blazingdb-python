"""
Defines the base stage class for use during data migration
"""

import flags

from blazingdb import exceptions


class When(flags.Flags):
    """ Defines the stages at which a custom query can be executed """

    before = ()
    after = ()

class BaseStage(object): # pylint: disable=too-few-public-methods
    """ Base class for all pipeline stages """

    async def _call(self, method, data):
        if hasattr(self, method):
            await getattr(self, method)(data)

    async def process(self, step, data):
        """ Processes the current stage """
        await self._call("before", data)

        try:
            async for item in step(data):
                yield item
        except Exception as ex:
            skipped = isinstance(ex, exceptions.SkipImportException)
            extra_data = {"skipped": skipped, "success": False}

            await self._call("after", {**data, **extra_data})
            raise

        await self._call("after", {**data, "success": True})
