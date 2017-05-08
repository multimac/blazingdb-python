"""
Defines a series of pipeline stages including:
 - CreateTableStage
 - DropTableStage
 - FilterColumnsStage
 - LimitImportStage
 - PostImportHackStage
 - PrefixTableStage
 - TruncateTableStage
"""

import json
import logging

import flags

from . import base
from .. import exceptions


# pragma pylint: disable=too-few-public-methods

class CreateTableStage(base.BaseStage):
    """ Creates the destination table before importing data into BlazingDB """

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    async def _create_table(connector, table, column_data):
        columns = ", ".join([
            "{0} {1}".format(column["name"], column["type"])
            for column in column_data
        ])

        await connector.query("CREATE TABLE {0} ({1})".format(table, columns))

    async def begin_import(self, data):
        """ Triggers the creation of the destination table """
        connector = data["connector"]
        columns = data["columns"]
        table = data["dest_table"]

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


class CustomQueryStage(base.BaseStage):
    """ Performs a query against BlazingDB and outputs the results to the log """

    class When(flags.Flags):
        """ Defines the stages at which a custom query can be executed """

        before = ()
        after = ()

    def __init__(self, query, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.query = query

        self.quiet = kwargs.get("quiet", False)
        self.when = kwargs.get("when", CustomQueryStage.When.before)

    async def _perform_query(self, data):
        connector = data["connector"]
        formatted_query = self.query.format(
            table=data["dest_table"]
        )

        results = await connector.query(formatted_query)

        self.logger.debug(
            "Reults for custom query stage: %s",
            json.dumps(results)
        )

    async def begin_import(self, data):
        """ Triggers the query if it should be run before the table is imported """
        if CustomQueryStage.When.before not in self.when:
            return

        await self._perform_query(data)

    async def end_import(self, data):
        """ Triggers the query if it should be run before the table is imported """
        if CustomQueryStage.When.after not in self.when:
            return

        await self._perform_query(data)


class DropTableStage(base.BaseStage):
    """ Drops the destination table before importing data into BlazingDB """

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    async def _drop_table(connector, table):
        await connector.query("DROP TABLE {0}".format(table))

    async def begin_import(self, data):
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

    def _filter_stream(self, columns, table, stream):
        ignored_columns = self.tables.get(table, [])

        current_start = None
        slices = []

        for index, column in enumerate(columns):
            if column["name"] not in ignored_columns:
                if current_start is None:
                    current_start = index

                continue

            if current_start is not None:
                slices.append(slice(current_start, index))
                current_start = None

        if current_start is not None:
            slices.append(slice(current_start, None))

        self.logger.debug(
            "Generated %s row segments for table %s",
            len(slices), table
        )

        for row in stream:
            filtered_row = []

            for row_slice in slices:
                filtered_row.extend(row[row_slice])

            yield filtered_row


    async def begin_import(self, data):
        """ Replaces the stream with one which filters the columns """
        ignored_columns = self.tables.get(data["src_table"], [])

        self.logger.info(
            "Filtering %s columns from %s%s", len(ignored_columns), data["src_table"],
            " ({0})".format(", ".join(ignored_columns)) if len(ignored_columns) != 0 else ""
        )

        data["stream"] = self._filter_stream(data["columns"], data["src_table"], data["stream"])

        filter_column = lambda col: col["name"] not in ignored_columns
        data["columns"] = list(filter(filter_column, data["columns"]))


class LimitImportStage(base.BaseStage):
    """ Limits the number of rows imported from the source """

    def __init__(self, count):
        self.logger = logging.getLogger(__name__)
        self.count = count

    def _limit_stream(self, stream):
        for index, row in enumerate(stream):
            if index >= self.count:
                self.logger.debug("Reached %s row limit, not returning any more rows", self.count)
                break

            yield row

        raise StopIteration

    async def begin_import(self, data):
        """ Replaces the stream with one which limits the number of rows returned """
        data["stream"] = self._limit_stream(data["stream"])


class PostImportHackStage(base.BaseStage):
    """ Performs a series of queries to help fix an issue with BlazingDB importing data """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    @staticmethod
    async def _perform_post_import_queries(connector, table):
        await connector.query("POST-OPTIMIZE TABLE {0}".format(table))
        await connector.query("GENERATE SKIP-DATA FOR {0}".format(table))

    async def end_import(self, data):
        """ Triggers the series of queries required to fix the issue """
        connector = data["connector"]
        table = data["dest_table"]

        self.logger.info("Performing post-optimize on table %s", table)
        await self._perform_post_import_queries(connector, table)


class PrefixTableStage(base.BaseStage):
    """ Prefixes the destination tables """

    def __init__(self, prefix):
        self.prefix = prefix

    async def begin_import(self, data):
        """ Prefixes the destination table with the given prefix """
        data["dest_table"] = "{0}_{1}".format(self.prefix, data["dest_table"])


class TruncateTableStage(base.BaseStage):
    """ Drops the destination table before importing data into BlazingDB """

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    async def _truncate_table(connector, table):
        await connector.query("DELETE FROM {0}".format(table))

    async def begin_import(self, data):
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
