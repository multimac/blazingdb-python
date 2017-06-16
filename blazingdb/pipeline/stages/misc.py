"""
Defines a series of miscellaneous pipeline stages, including:
 - DelayStage
 - PrefixTableStage
 - PromptInputStage
"""

import asyncio
import fnmatch

from blazingdb import exceptions
from . import base, custom


# pragma pylint: disable=too-few-public-methods

class DelayStage(custom.CustomActionStage):
    """ Pauses the pipeline before / after importing data """

    def __init__(self, delay, **kwargs):
        super(DelayStage, self).__init__(self._delay, **kwargs)
        self.prompt = kwargs.get("prompt", "Waiting for input...")
        self.delay = delay

    async def _delay(self, data):  # pylint: disable=unused-argument
        await asyncio.sleep(self.delay)


class PromptInputStage(custom.CustomActionStage):
    """ Prompts for user input to continue before / after importing data """

    def __init__(self, **kwargs):
        super(PromptInputStage, self).__init__(self._prompt, **kwargs)
        self.prompt = kwargs.get("prompt", "Waiting for input...")

    async def _prompt(self, data):  # pylint: disable=unused-argument
        input(self.prompt)


class SemaphoreStage(base.BaseStage):
    """ Uses a semaphore to prevent access to later parts of the pipeline """

    def __init__(self, limit, loop=None):
        super(SemaphoreStage, self).__init__()
        self.semaphore = asyncio.BoundedSemaphore(limit, loop=loop)

    async def process(self, step, data):
        async with self.semaphore:
            async for item in step():
                yield item


class SkipImportStage(base.BaseStage):
    """ Skips the import if this stage is reached """

    async def before(self, data):  # pylint: disable=unused-argument
        """ Raises a SkipImportException to skip the import of data """
        raise exceptions.SkipImportException()


class SkipTableStage(base.BaseStage):
    """ Skips tables based on a given set of inclusions and exclusions """

    def __init__(self, included=None, excluded=None):
        super(SkipTableStage, self).__init__()

        self.included = included
        self.excluded = excluded

    def _filter_table(self, table):
        if self.excluded is not None:
            for pattern in self.excluded:
                if fnmatch.fnmatch(table, pattern):
                    return True

        if self.included is None:
            return False

        for pattern in self.included:
            if fnmatch.fnmatch(table, pattern):
                return False

        return True

    async def before(self, data):
        """ Raises a SkipImportException if the table should be filtered """
        if self._filter_table(data["src_table"]):
            raise exceptions.SkipImportException()
