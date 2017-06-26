"""
Defines the MultiprocessStage and it's respective classes
"""

import asyncio
import concurrent

from . import base
from .. import messages, system


class MultiprocessStage(base.BaseStage):
    """ Passes messages off to multiple processes for handling """

    def __init__(self, *stages, loop=None):
        super(MultiprocessStage, self).__init__(messages.Packet)

        self.executor = concurrent.futures.ProcessPoolExecutor()
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.stages = stages

    async def process(self, message):
        await self.loop.run_in_executor(self.executor, multiprocess_stage, self.stages, message)

def multiprocess_stage(stages, message):
    """ Builds a new System with the given stages to process the message """
    stages.insert(0, MultiprocessStage)

    loop = asyncio.get_event_loop()
    local_system = system.System(stages)

    process_task = asyncio.ensure_future(local_system.process(message), loop=loop)

    return loop.run_until_complete(process_task)
