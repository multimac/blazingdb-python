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

class CustomActionStage(base.BaseStage):
    """ Performs a custom callback before / after importing data """

    def __init__(self, callback, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.callback = callback
        self.when = kwargs.get("when", When.before)

    async def _perform_callback(self, data):
        await self.callback(data)

    async def before(self, data):
        """ Triggers the callback if it has queued to run before the import """
        if When.before not in self.when:
            return

        await self._perform_callback(data)

    async def after(self, data):
        """ Triggers the callback if it has queued to run after the import """
        if When.after not in self.when:
            return

        await self._perform_callback(data)


class CustomCommandStage(CustomActionStage):
    """ Runs a sub-process before / after importing data """

    def __init__(self, program, *args, **kwargs):
        super(CustomCommandStage, self).__init__(self._perform_command, **kwargs)
        self.logger = logging.getLogger(__name__)

        self.program = program
        self.args = args

    async def _perform_command(self, data):  # pylint: disable=unused-argument
        self.logger.info("Performing command: %s", " ".join([str(a) for a in self.args]))

        null = asyncio.subprocess.DEVNULL
        process = await asyncio.create_subprocess_exec(
            self.program, *self.args,
            stdin=null, stdout=null, stderr=null
        )

        await process.wait()


class CustomQueryStage(CustomActionStage):
    """ Performs a query against BlazingDB before / after importing data """

    def __init__(self, query, **kwargs):
        super(CustomQueryStage, self).__init__(self._perform_query, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.query = query

    async def _perform_query(self, data):
        connector = data["connector"]
        table = data["dest_table"]

        formatted_query = self.query.format(table=table)
        results = await connector.query(formatted_query)

        self.logger.debug("Reults for custom query stage: %s", json.dumps(results))
