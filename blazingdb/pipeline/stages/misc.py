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
from .. import messages


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


class SkipImportStage(base.BaseStage):
    """ Skips the import if this stage is reached """

    def __init__(self):
        super(SkipImportStage, self).__init__(messages.ImportTablePacket)

    async def process(self, message):  # pylint: disable=unused-argument
        """ Ignores the message to prevent it being forwarded on """


class SkipTableStage(base.BaseStage):
    """ Skips tables based on a given set of inclusions and exclusions """

    def __init__(self, included=None, excluded=None):
        super(SkipTableStage, self).__init__(messages.ImportTablePacket)

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

    async def process(self, message):
        """ Only calls message.forward if the message isn't filtered """
        if not self._filter_table(message.src_table):
            await message.forward()
