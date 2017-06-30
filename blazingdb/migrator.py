"""
Defines the Migrator class which can be used for migrating data into BlazingDB
"""

import asyncio
import logging

from blazingdb.pipeline import messages


class Migrator(object):
    """ Handles migrating data from a source into BlazingDB """

    def __init__(self, triggers, pipeline, destination, loop=None):
        self.logger = logging.getLogger(__name__)

        self.loop = loop

        self.triggers = triggers
        self.pipeline = pipeline
        self.destination = destination

    async def _poll_trigger(self, trigger):
        """ Polls a trigger, placing any returned messages on the queue """
        async for message in trigger.poll():
            message.add_packet(messages.DestinationPacket(self.destination))

            await self.pipeline.enqueue(message)

            self.logger.info("Added message %s to the pipeline", message)

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
        await self.pipeline.shutdown()

        self.logger.debug("Migrator successfully shutdown")
