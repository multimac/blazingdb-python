"""
Defines a series of pipeline stages for performing custom actions, including:
 - CustomActionStage
 - CustomCommandStage
 - CustomQueryStage
"""

import asyncio
import json
import logging

from . import base, When
from .. import packets


class CustomActionStage(base.PipelineStage):
    """ Performs a custom callback before / after importing data """

    def __init__(self, callback, **kwargs):
        super(CustomActionStage, self).__init__(packets.ImportTablePacket)
        self.callback = callback

        self.when = kwargs.get("when", When.before)

    async def _perform_callback(self, message):
        await self.callback(message)

    async def before(self, message):
        """ Triggers the callback if it has queued to run before the import """
        if When.before not in self.when:
            return

        await self._perform_callback(message)

    async def after(self, message, success):  # pylint: disable=unused-argument
        """ Triggers the callback if it has queued to run after the import """
        if When.after not in self.when:
            return

        await self._perform_callback(message)


class CustomCommandStage(CustomActionStage):
    """ Runs a sub-process before / after importing data """

    def __init__(self, program, *args, **kwargs):
        super(CustomCommandStage, self).__init__(self._perform_command, **kwargs)
        self.logger = logging.getLogger(__name__)

        self.program = program
        self.args = args

    async def _perform_command(self, message):  # pylint: disable=unused-argument
        self.logger.info("Performing command: %s", " ".join([str(a) for a in self.args]))

        null = asyncio.subprocess.DEVNULL
        process = await asyncio.create_subprocess_exec(
            self.program, *self.args, stdin=null, stdout=null, stderr=null)

        await process.wait()


class CustomQueryStage(CustomActionStage):
    """ Performs a query against BlazingDB before / after importing data """

    def __init__(self, query, **kwargs):
        super(CustomQueryStage, self).__init__(self._perform_query, **kwargs)
        self.logger = logging.getLogger(__name__)

        self.query = query

    async def _perform_query(self, message):
        import_pkt = message.get_packet(message.ImportTablePacket)
        dest_pkt = message.get_packet(message.DestinationPacket)

        destination = dest_pkt.destination
        table = import_pkt.table

        identifier = destination.get_identifier(table)
        formatted_query = self.query.format(table=identifier)

        results = [item async for chunk in destination.query(formatted_query) for item in chunk]

        self.logger.debug("Reults for custom query stage: %s", json.dumps(results))
