"""
Defines classes involved in running stages of a pipeline
"""

import asyncio
import logging

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

    def __init__(self, *stages, loop=None, processor_count=5):
        self.logger = logging.getLogger(__name__)

        self.stages = list(stages) + [System.BlackholeStage()]
        self.tracker = dict()

        self.processor = processor.Processor(
            self._process_message, loop=loop, enqueue_while_stopping=True,
            processor_count=processor_count, queue_length=0)

    def _trigger_futures(self, message):
        self.logger.debug("Completing futures for %s", message)

        for packet in message.get_packets(packets.FuturePacket):
            packet.future.set_result(None)

    async def _process_message(self, message):
        self.tracker[message.msg_id] -= 1

        await self.stages[message.stage_idx].receive(message)

        if self.tracker.get(message.msg_id) <= 0:
            self._trigger_futures(message)

    async def enqueue(self, message):
        """ Queues a given message to be processed """
        self.tracker.setdefault(message.msg_id, 0)
        self.tracker[message.msg_id] += 1

        message.system = self

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
