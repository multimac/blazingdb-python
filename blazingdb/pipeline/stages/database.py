"""
Defines a series of pipeline stages for affecting database tables, including:
 - CreateTableStage
 - DropTableStage
 - TruncateTableStage
"""

import logging

from blazingdb import exceptions
from blazingdb.util.blazing import build_datatype

from . import base, custom
from .. import messages


# pragma pylint: disable=too-few-public-methods

class CreateTableStage(base.PipelineStage):
    """ Creates the destination table before importing data """

    def __init__(self, **kwargs):
        super(CreateTableStage, self).__init__(messages.ImportTablePacket)
        self.logger = logging.getLogger(__name__)

        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    async def _get_columns(packet):
        return await packet.source.get_columns(packet.src_table)

    @staticmethod
    async def _create_table(destination, table, column_data):
        columns = ", ".join([
            "{0} {1}".format(column.name, build_datatype(column))
            for column in column_data
        ])

        identifier = destination.get_identifier(table)
        query = "CREATE TABLE {0} ({1})".format(identifier, columns)

        await destination.execute(query)

    async def before(self, message):
        """ Triggers the creation of the destination table """
        packet = message.get_packet(messages.ImportTablePacket)

        destination = packet.destination
        table = packet.dest_table

        columns = await self._get_columns(packet)

        self.logger.info("Creating table %s with %s column(s)", table, len(columns))

        try:
            await self._create_table(destination, table, columns)
        except exceptions.QueryException as ex:
            if not self.quiet:
                raise

            self.logger.debug(" ".join([
                "QueryException caught when creating table %s,",
                "ignoring as it most likely means the table exists"
            ]), table)

            self.logger.debug(ex.response)


class DropTableStage(base.PipelineStage):
    """ Drops the destination table before importing data """

    def __init__(self, **kwargs):
        super(DropTableStage, self).__init__(messages.ImportTablePacket)
        self.logger = logging.getLogger(__name__)

        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    async def _drop_table(destination, table):
        identifier = destination.get_identifier(table)
        await destination.execute("DROP TABLE {0}".format(identifier))

    async def before(self, message):
        """ Triggers the dropping of the destination table """
        packet = message.get_packet(messages.ImportTablePacket)

        destination = packet.destination
        table = packet.dest_table

        self.logger.info("Dropping table %s", table)

        try:
            await self._drop_table(destination, table)
        except exceptions.QueryException as ex:
            if not self.quiet:
                raise

            self.logger.debug(" ".join([
                "QueryException caught when dropping table %s,",
                "ignoring as it most likely means the table doesn't exist"
            ]), table)

            self.logger.debug(ex.response)


class PostImportHackStage(base.PipelineStage):
    """ Performs a series of queries to help fix an issue with importing data """

    def __init__(self, **kwargs):
        super(PostImportHackStage, self).__init__(messages.ImportTablePacket)
        self.logger = logging.getLogger(__name__)

        self.perform_on_failure = kwargs.get("perform_on_failure", False)

    @staticmethod
    async def _perform_post_import_queries(destination, table):
        identifier = destination.get_identifier(table)

        await destination.execute("POST-OPTIMIZE TABLE {0}".format(identifier))
        await destination.execute("GENERATE SKIP-DATA FOR {0}".format(identifier))

    async def after(self, message, skipped, success):
        """ Triggers the series of queries required to fix the issue """
        failed = not(success or skipped)
        if failed and not self.perform_on_failure:
            return

        packet = message.get_message(messages.ImportTablePacket)

        destination = packet.destination
        table = packet.dest_table

        self.logger.info("Performing post-optimize on table %s", table)
        await self._perform_post_import_queries(destination, table)


class SourceComparisonStage(custom.CustomActionStage):
    """ Performs queries against both BlazingDB and the given source, and compares the results """

    def __init__(self, query, **kwargs):
        super(SourceComparisonStage, self).__init__(self._perform_comparison, **kwargs)
        self.logger = logging.getLogger(__name__)

        self.query = query

    @staticmethod
    def _compare_rows(left, right):
        if len(left) != len(right):
            return False

        different = False
        for left_cell, right_cell in zip(left, right):
            different |= (left_cell != right_cell)

        return different

    def _compare_results(self, left, right):
        if len(left) != len(right):
            return False

        different = False
        for left_row, right_row in zip(left, right):
            different |= self._compare_rows(left_row, right_row)

        return different

    async def _query_source(self, source, table, column):
        identifier = source.get_identifier(table)
        formatted_query = self.query.format(table=identifier, column=column)

        return [item async for chunk in source.query(formatted_query) for item in chunk]

    async def _perform_comparison(self, message):
        """ Performs the queries after data has been imported """
        packet = message.get_packet(messages.ImportTablePacket)

        destination = packet.destination
        dest_table = packet.dest_table
        src_table = packet.src_table
        source = packet.source

        columns = await destination.get_columns(src_table)
        misc_column = columns[0].name

        dest_results = await self._query_source(destination, dest_table, misc_column)
        src_results = await self._query_source(source, src_table, misc_column)

        different = self._compare_results(dest_results, src_results)

        if not different:
            return

        self.logger.warning(" ".join([
            "Comparison query on table %s differed",
            "between BlazingDB and the source"
        ]), src_table)

        self.logger.debug("Destination: %s", dest_results)
        self.logger.debug("Source: %s", src_results)


class TruncateTableStage(base.PipelineStage):
    """ Deletes all rows in the destination table before importing data """

    def __init__(self, **kwargs):
        super(TruncateTableStage, self).__init__(messages.ImportTablePacket)
        self.logger = logging.getLogger(__name__)

        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    async def _truncate_table(destination, table):
        identifier = destination.get_identifier(table)
        await destination.execute("DELETE FROM {0}".format(identifier))

    async def before(self, message):
        """ Triggers the truncation of the destination table """
        packet = message.get_packet(messages.ImportTablePacket)

        destination = packet.destination
        table = packet.dest_table

        self.logger.info("Truncating table %s", table)

        try:
            await self._truncate_table(destination, table)
        except exceptions.QueryException as ex:
            if not self.quiet:
                raise

            self.logger.debug(" ".join([
                "QueryException caught when truncating table %s,",
                "ignoring as it most likely means the table is already empty"
            ]), table)

            self.logger.debug(ex.response)
