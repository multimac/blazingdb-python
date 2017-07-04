"""
Defines classes involved in running stages of a pipeline
"""

import asyncio

from blazingdb import processor

from . import packets
from .stages import base


class System(object):
    """ Wraps an array of pipeline stages """

    class BlackholeStage(base.BaseStage):
        """ Empty stage to prevent messages continuing past end of pipeline """

        async def process(self, message):
            pass

        async def receive(self, message):
            pass

    class TriggerStage(base.BaseStage):
        """ Triggers any futures configured on the message """

        def __init__(self):
            super(System.TriggerStage, self).__init__(packets.FuturePacket)

        async def process(self, message):
            for packet in message.get_packets(packets.FuturePacket):
                packet.future.set_result(None)

    def __init__(self, *stages, loop=None, processor_count=5):
        self.stages = list(stages) + [System.TriggerStage, System.BlackholeStage()]

        self.processor = processor.Processor(
            self._process_message, loop=loop, enqueue_while_stopping=True,
            processor_count=processor_count, queue_length=0)

    async def _process_message(self, message):
        await message.transport.send(self.stages[message.stage_idx])

    async def enqueue(self, message):
        """ Queues a given message to be processed """
        await self.processor.enqueue(message)

    async def process(self, message, loop=None):
        """ Queues and waits for a given message to be processed """
        loop = loop if loop is not None else asyncio.get_event_loop()

        future = loop.create_future()
        packet = packets.FuturePacket(future)

        message.add_packet(packet)

        await self.enqueue(message)
        await future

    async def shutdown(self):
        await self.processor.shutdown()
