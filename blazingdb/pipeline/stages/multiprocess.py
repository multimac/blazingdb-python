"""
Defines the MultiprocessStage and it's respective classes
"""

import asyncio
import concurrent

from . import base
from .. import messages, system


class CallbackStage(base.BaseStage):
    """ Triggers a given callback with any message it's given """

    def __init__(self, callback):
        super(CallbackStage, self).__init__(messages.Packet)
        self.callback = callback

    async def process(self, message):
        self.callback(message)


class MultiprocessStage(base.BaseStage):
    """ Passes messages off to multiple processes for handling """

    def __init__(self, *stages, loop=None):
        super(MultiprocessStage, self).__init__(messages.Packet)

        self.executor = concurrent.futures.ProcessPoolExecutor()
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.stages = stages

    async def process(self, message):
        await self.loop.run_in_executor(self.executor, multiprocess_main, self.stages, message)

async def multiprocess_stage(stages, message, loop):
    """ Builds a new System with the given stages and runs the message through it """
    future = loop.create_future()

    stages.insert(0, MultiprocessStage)
    stages.append(CallbackStage(future.set_result))

    local_system = system.System(stages)

    asyncio.ensure_future(local_system.process(message), loop=loop)

    loop.run_until_complete(future)
    return future.result()

def multiprocess_main(stages, message):
    """ Runs a message through the given stages """
    loop = asyncio.new_event_loop()

    try:
        return multiprocess_stage(stages, message, loop)
    finally:
        shutdown_task = loop.shutdown_asyncgens()

        loop.run_until_complete(shutdown_task)
        loop.close()
