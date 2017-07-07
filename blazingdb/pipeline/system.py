"""
Defines classes involved in running stages of a pipeline
"""

import asyncio
import logging

from .stages import base


class System(object):
    """ Wraps an array of pipeline stages """

    class BlackholeStage(base.BaseStage):
        """ Empty stage to prevent messages continuing past end of pipeline """

        async def process(self, message):
            pass

    def __init__(self, *stages, loop=None):
        self.logger = logging.getLogger(__name__)

        self.loop = loop
        self.stages = list(stages) + [System.BlackholeStage()]
        self.tasks = set()

    async def _process_message(self, message):
        await self.stages[message.stage_idx].receive(message)
        self.tasks.remove(asyncio.Task.current_task())

    async def enqueue(self, message):
        """ Queues a given message to be processed """
        message.system = self

        coroutine = self._process_message(message)
        task = asyncio.ensure_future(coroutine, loop=self.loop)

        self.tasks.add(task)

    async def shutdown(self):
        """ Waits for all pending tasks and shuts down the stages """
        shutdown_tasks = [stage.shutdown() for stage in self.stages]

        while self.tasks:
            self.tasks, pending = [], self.tasks

            await asyncio.gather(pending,
                loop=self.loop, return_exceptions=True)

        await asyncio.gather(shutdown_tasks,
            loop=self.loop, return_exceptions=True)
