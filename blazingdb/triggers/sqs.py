"""
Defines a trigger which retrieves tables to import from an Amazon SQS queue
"""

import asyncio

from . import base


class SnsTrigger(base.TableTrigger):
    """ A trigger which returns tables from an Amazon SQS queue """

    DEFAULT_DELAY = 300

    def __init__(self, source, queue, **kwargs):
        super(SnsTrigger, self).__init__(source)
        self.delay = kwargs.get("delay", self.DEFAULT_DELAY)
        self.queue = queue

    async def _poll(self):
        while True:
            message = self.queue.read()

            if message is None:
                await self._wait()
                continue

            yield message.get_body()

    async def _wait(self):
        await asyncio.sleep(self.delay)
