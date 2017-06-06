"""
Defines a series of pipeline stages including:
 - CreateTableStage
 - CustomActionStage
 - CustomCommandStage
 - CustomQueryStage
 - DelayStage
 - DropTableStage
 - FilterColumnsStage
 - LimitImportStage
 - PromptInputStage
 - PostImportHackStage
 - PrefixTableStage
 - TruncateTableStage
"""

import asyncio
import json
import logging

import flags

from . import base, sources
from .. import exceptions


# pragma pylint: disable=too-few-public-methods

class When(flags.Flags):
    """ Defines the stages at which a custom query can be executed """

    before = ()
    after = ()

class CreateTableStage(base.BaseStage):
    """ Creates the destination table before importing data """

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    def _get_columns(data):
        source = data["source"]
        table = data["src_table"]

        return source.get_columns(table)

    @staticmethod
    async def _create_table(connector, table, column_data):
        columns = ", ".join([
            "{0} {1}".format(column["name"], column["type"])
            for column in column_data
        ])

        await connector.query("CREATE TABLE {0} ({1})".format(table, columns))

    async def before(self, data):
        """ Triggers the creation of the destination table """
        connector = data["connector"]
        table = data["dest_table"]

        columns = self._get_columns(data)

        self.logger.info("Creating table %s with %s column(s)", table, len(columns))

        try:
            await self._create_table(connector, table, columns)
        except exceptions.QueryException as ex:
            if not self.quiet:
                raise

            self.logger.debug(" ".join([
                "QueryException caught when creating table %s, ignoring as it most likely",
                "means the table exists"
            ]), table)
            self.logger.debug(ex.response)


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
        formatted_query = self.query.format(
            table=data["dest_table"]
        )

        results = await connector.query(formatted_query)

        self.logger.debug("Reults for custom query stage: %s", json.dumps(results))


class DelayStage(CustomActionStage):
    """ Pauses the pipeline before / after importing data """

    def __init__(self, delay, **kwargs):
        super(DelayStage, self).__init__(self._delay, **kwargs)
        self.prompt = kwargs.get("prompt", "Waiting for input...")
        self.delay = delay

    async def _delay(self, data):  # pylint: disable=unused-argument
        await asyncio.sleep(self.delay)


class DropTableStage(base.BaseStage):
    """ Drops the destination table before importing data """

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    async def _drop_table(connector, table):
        await connector.query("DROP TABLE {0}".format(table))

    async def before(self, data):
        """ Triggers the dropping of the destination table """
        connector = data["connector"]
        table = data["dest_table"]

        self.logger.info("Dropping table %s", table)

        try:
            await self._drop_table(connector, table)
        except exceptions.QueryException as ex:
            if not self.quiet:
                raise

            self.logger.debug(" ".join([
                "QueryException caught when dropping table %s, ignoring as it most likely",
                "means the table doesn't exist"
            ]), table)
            self.logger.debug(ex.response)


class FilterColumnsStage(base.BaseStage):
    """ Filters the given columns from the imported data """

    def __init__(self, tables):
        self.logger = logging.getLogger(__name__)
        self.tables = tables

    async def before(self, data):
        """ Replaces the stream with one which filters the columns """
        ignored_columns = self.tables.get(data["src_table"], [])

        self.logger.info(
            "Filtering %s columns from %s%s", len(ignored_columns), data["src_table"],
            " ({0})".format(", ".join(ignored_columns)) if ignored_columns else ""
        )

        data["source"] = sources.FilteredSource(data["source"], ignored_columns)


class LimitImportStage(base.BaseStage):
    """ Limits the number of rows imported from the source """

    def __init__(self, count):
        self.count = count

    async def before(self, data):
        """ Replaces the source with one which limits the number of rows returned """
        data["source"] = sources.limited.LimitedSource(data["source"], self.count)


class PromptInputStage(CustomActionStage):
    """ Prompts for user input to continue before / after importing data """

    def __init__(self, **kwargs):
        super(PromptInputStage, self).__init__(self._prompt, **kwargs)
        self.prompt = kwargs.get("prompt", "Waiting for input...")

    async def _prompt(self, data):  # pylint: disable=unused-argument
        input(self.prompt)


class PostImportHackStage(base.BaseStage):
    """ Performs a series of queries to help fix an issue with importing data """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    @staticmethod
    async def _perform_post_import_queries(connector, table):
        await connector.query("POST-OPTIMIZE TABLE {0}".format(table))
        await connector.query("GENERATE SKIP-DATA FOR {0}".format(table))

    async def after(self, data):
        """ Triggers the series of queries required to fix the issue """
        connector = data["connector"]
        table = data["dest_table"]

        self.logger.info("Performing post-optimize on table %s", table)
        await self._perform_post_import_queries(connector, table)


class PrefixTableStage(base.BaseStage):
    """ Prefixes the destination tables """

    def __init__(self, prefix, separator="$"):
        self.prefix = prefix
        self.separator = separator

    async def before(self, data):
        """ Prefixes the destination table with the given prefix """
        data["dest_table"] = self.separator.join(self.prefix, data["dest_table"])


class TruncateTableStage(base.BaseStage):
    """ Deletes all rows in the destination table before importing data """

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    async def _truncate_table(connector, table):
        await connector.query("DELETE FROM {0}".format(table))

    async def before(self, data):
        """ Triggers the truncation of the destination table """
        connector = data["connector"]
        table = data["dest_table"]

        self.logger.info("Truncating table %s", table)

        try:
            await self._truncate_table(connector, table)
        except exceptions.QueryException as ex:
            if not self.quiet:
                raise

            self.logger.debug(" ".join([
                "QueryException caught when truncating table %s, ignoring as it most likely",
                "means the table is already empty"
            ]), table)
            self.logger.debug(ex.response)
