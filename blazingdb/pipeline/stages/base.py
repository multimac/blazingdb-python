"""
Defines the base stage class for use during data migration
"""

import flags
import logging

from blazingdb import exceptions


class When(flags.Flags):
    """ Defines the stages at which a custom query can be executed """

    before = ()
    after = ()

class BaseStage(object): # pylint: disable=too-few-public-methods
    """ Base class for all pipeline stages """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    async def _call(self, method, data):
        if hasattr(self, method):
            await getattr(self, method)(data)

    async def process(self, step, data):
        """ Processes the current stage """
        await self._call("before", data)

        try:
            async for item in step():
                yield item
        except Exception as ex:
            skipped = isinstance(ex, exceptions.SkipImportException)
            extra_data = {"skipped": skipped, "success": False}

            try:
                await self._call("after", {**data, **extra_data})
            except Exception:  # pylint: disable=broad-except
                self.logger.exception("Failed calling 'after' during exception handler")

            raise

        extra_data = {"skipped": False, "success": True}
        await self._call("after", {**data, **extra_data})
