"""
Defines a trigger which retrieves tables to import from an Amazon SQS queue
"""

import asyncio

from blazingdb.pipeline import messages

from . import base


class SnsTrigger(base.BaseTrigger):  # pylint: disable=too-few-public-methods
    """ A trigger which returns tables from an Amazon SQS queue """

    DEFAULT_DELAY = 300

    def __init__(self, queue, source, **kwargs):
        self.queue = queue
        self.source = source

        self.delay = kwargs.get("delay", self.DEFAULT_DELAY)

    async def _wait(self):
        await asyncio.sleep(self.delay)

    async def poll(self):
        while True:
            message = self.queue.read()

            if message is None:
                await self._wait()
                continue

            yield messages.Message(messages.ImportTablePacket(self.source, message.get_body()))
