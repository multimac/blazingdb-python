"""
Defines a series of pipeline stages for affecting database tables, including:
 - CreateTableStage
 - DropTableStage
 - TruncateTableStage
"""

import logging

from blazingdb import exceptions
from blazingdb.util.blazing import build_datatype

from . import custom
from .. import packets
from ..util import get_columns


class CreateTableStage(custom.CustomActionStage):
    """ Creates the destination table before importing data """

    def __init__(self, **kwargs):
        super(CreateTableStage, self).__init__(
            self._create_table, packets.ImportTablePacket, **kwargs)

        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    async def _perform_query(destination, table, columns):
        query_columns = ", ".join([
            "{0} {1}".format(column.name, build_datatype(column))
            for column in columns
        ])

        identifier = destination.get_identifier(table)
        query = "CREATE TABLE {0} ({1})".format(identifier, query_columns)

        await destination.execute(query)

    async def _create_table(self, message):
        """ Triggers the creation of the destination table """
        import_pkt = message.get_packet(packets.ImportTablePacket)
        dest_pkt = message.get_packet(packets.DestinationPacket)

        table = import_pkt.table
        destination = dest_pkt.destination
        columns = await get_columns(message, add_if_missing=True)

        self.logger.info("Creating table %s with %s column(s)", table, len(columns))

        try:
            await self._perform_query(destination, table, columns)
        except exceptions.QueryException as ex:
            if not self.quiet:
                raise

            self.logger.debug(" ".join([
                "QueryException caught when creating table %s,",
                "ignoring as it most likely means the table exists"
            ]), table)

            self.logger.debug(ex.response)


class DropTableStage(custom.CustomActionStage):
    """ Drops the destination table before importing data """

    def __init__(self, **kwargs):
        super(DropTableStage, self).__init__(
            self._drop_table, packets.ImportTablePacket, **kwargs)

        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", False)

    async def _drop_table(self, message):
        """ Triggers the dropping of the destination table """
        import_pkt = message.get_packet(packets.ImportTablePacket)
        dest_pkt = message.get_packet(packets.DestinationPacket)

        table = import_pkt.table
        destination = dest_pkt.destination
        identifier = destination.get_identifier(table)

        self.logger.info("Dropping table %s", table)

        try:
            await destination.execute("DROP TABLE {0}".format(identifier))
        except exceptions.QueryException as ex:
            if not self.quiet:
                raise

            self.logger.debug(" ".join([
                "QueryException caught when dropping table %s,",
                "ignoring as it most likely means the table doesn't exist"
            ]), table)

            self.logger.debug(ex.response)


class SourceComparisonStage(custom.CustomActionStage):
    """ Performs queries against both BlazingDB and the given source, and compares the results """

    def __init__(self, query, **kwargs):
        super(SourceComparisonStage, self).__init__(
            self._perform_comparison, packets.ImportTablePacket, **kwargs)

        self.logger = logging.getLogger(__name__)
        self.query = query

    @staticmethod
    def _compare_results(left_set, right_set):
        def _compare_rows(left_row, right_row):
            if len(left_row) != len(right_row):
                return False

            return all([l == r for l, r in zip(left_row, right_row)])

        if len(left_set) != len(right_set):
            return False

        return all([_compare_rows(l, r) for l, r in zip(left_set, right_set)])

    async def _query_source(self, source, table, column):
        identifier = source.get_identifier(table)

        formatted_query = self.query.format(table=identifier, column=column)
        return [item async for chunk in source.query(formatted_query) for item in chunk]

    async def _perform_comparison(self, message):
        """ Performs the queries after data has been imported """
        import_pkt = message.get_packet(packets.ImportTablePacket)
        dest_pkt = message.get_packet(packets.DestinationPacket)

        table = import_pkt.table
        source = import_pkt.source
        destination = dest_pkt.destination

        columns = await get_columns(message)
        src_results = await self._query_source(source, table, columns[0].name)
        dest_results = await self._query_source(destination, table, columns[0].name)

        different = self._compare_results(dest_results, src_results)

        if not different:
            return False

        self.logger.warning(" ".join([
            "Comparison query on table %s differed",
            "between the source and the destination"
        ]), table)

        self.logger.debug("Source: %s", src_results)
        self.logger.debug("Destination: %s", dest_results)


class TruncateTableStage(custom.CustomActionStage):
    """ Deletes all rows in the destination table before importing data """

    def __init__(self, **kwargs):
        super(TruncateTableStage, self).__init__(
            self._truncate_table, packets.ImportTablePacket, **kwargs)

        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", False)

    async def _truncate_table(self, message):
        """ Triggers the truncation of the destination table """
        import_pkt = message.get_packet(packets.ImportTablePacket)
        dest_pkt = message.get_packet(packets.DestinationPacket)

        table = import_pkt.table
        destination = dest_pkt.destination
        identifier = destination.get_identifier(table)

        self.logger.info("Truncating table %s", table)

        try:
            await destination.execute("DELETE FROM {0}".format(identifier))
        except exceptions.QueryException as ex:
            if not self.quiet:
                raise

            self.logger.debug(" ".join([
                "QueryException caught when truncating table %s,",
                "ignoring as it most likely means the table is already empty"
            ]), table)

            self.logger.debug(ex.response)
