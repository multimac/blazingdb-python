"""
Defines a series of pipeline stages including:
 - CreateTableStage
 - DropTableStage
 - LimitImportStage
 - PrefixTableStage
 - TruncateTableStage
"""

import logging

from . import base
from .. import exceptions


# pragma pylint: disable=too-few-public-methods, unused-argument

class CreateTableStage(base.BaseStage):
    """ Creates the destination table before importing data into BlazingDB """

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    def _create_table(connector, table, column_data):
        columns = ", ".join([
            "{0} {1}".format(column["name"], column["type"])
            for column in column_data
        ])

        connector.query("CREATE TABLE {0} ({1})".format(table, columns), auto_connect=True)

    def begin_import(self, source, importer, connector, data):
        """ Triggers the creation of the destination table """
        columns = source.get_columns(data["src_table"])
        table = data["dest_table"]

        self.logger.info("Creating table %s with %s column(s)", table, len(columns))

        try:
            self._create_table(connector, table, columns)
        except exceptions.QueryException as ex:
            if not self.quiet:
                raise

            self.logger.debug(" ".join([
                "QueryException caught when creating table %s, ignoring as it most likely",
                "means the table exists"
            ]), table)
            self.logger.debug(ex.response)


class DropTableStage(base.BaseStage):
    """ Drops the destination table before importing data into BlazingDB """

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    def _drop_table(connector, table):
        connector.query("DROP TABLE {0}".format(table), auto_connect=True)

    def begin_import(self, source, importer, connector, data):
        """ Triggers the dropping of the destination table """
        table = data["dest_table"]

        self.logger.info("Dropping table %s", table)

        try:
            self._drop_table(connector, table)
        except exceptions.QueryException as ex:
            if not self.quiet:
                raise

            self.logger.debug(" ".join([
                "QueryException caught when dropping table %s, ignoring as it most likely",
                "means the table doesn't exist"
            ]), table)
            self.logger.debug(ex.response)


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

    def begin_import(self, source, importer, connector, data):
        """ Replaces the stream with one which limits the number of rows returned """
        data["stream"] = self._limit_stream(data["stream"])


class PrefixTableStage(base.BaseStage):
    """ Prefixes the destination tables """

    def __init__(self, prefix):
        self.prefix = prefix

    def begin_import(self, source, importer, connector, data):
        """ Prefixes the destination table with the given prefix """
        data["dest_table"] = "{0}_{1}".format(self.prefix, data["dest_table"])


class TruncateTableStage(base.BaseStage):
    """ Drops the destination table before importing data into BlazingDB """

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", False)

    @staticmethod
    def _truncate_table(connector, table):
        connector.query("DELETE FROM {0}".format(table), auto_connect=True)

    def begin_import(self, source, importer, connector, data):
        """ Triggers the truncation of the destination table """
        table = data["dest_table"]

        self.logger.info("Truncating table %s", table)

        try:
            self._truncate_table(connector, table)
        except exceptions.QueryException as ex:
            if not self.quiet:
                raise

            self.logger.debug(" ".join([
                "QueryException caught when truncating table %s, ignoring as it most likely",
                "means the table is already empty"
            ]), table)
            self.logger.debug(ex.response)
