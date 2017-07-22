"""
Defines the Migrator class which can be used for migrating data into BlazingDB
"""

import asyncio
import logging

from blazingdb.pipeline import packets

from . import processor


class Migrator(object):
    """ Handles migrating data from a source into BlazingDB """

    DEFAULT_PROCESSOR_COUNT = 5

    def __init__(self, triggers, pipeline, destination, loop=None, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.loop = loop
        self.triggers = triggers
        self.pipeline = pipeline
        self.destination = destination

        processor_count = kwargs.get("processor_count", Migrator.DEFAULT_PROCESSOR_COUNT)
        self.processor = processor.Processor(self._process_import,
            loop=loop, processor_count=processor_count)

    async def _process_import(self, message):
        """ Processes an import message """
        self.logger.info("Running message %s through the pipeline", message)

        await self.pipeline.enqueue(message)
        await message.handle

    async def _poll_trigger(self, trigger):
        """ Polls a trigger, placing any returned messages on the queue """
        async for message in trigger.poll():
            message.add_packet(packets.DestinationPacket(self.destination))

            await self.processor.enqueue(message)

    async def migrate(self):
        """ Begins polling triggers and processing any messages returned from them """
        poll_tasks = [self._poll_trigger(trigger) for trigger in self.triggers]
        gathered = asyncio.gather(*poll_tasks, loop=self.loop)

        try:
            await gathered
        except:
            gathered.cancel()
            raise

    async def shutdown(self):
        """ Shuts down the migrator, cancelling any currently polled triggers """
        await self.processor.shutdown()
        await self.pipeline.shutdown()

        self.logger.debug("Migrator successfully shutdown")
