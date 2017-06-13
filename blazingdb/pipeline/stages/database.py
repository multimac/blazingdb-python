"""
Defines a series of pipeline stages for affecting database tables, including:
 - CreateTableStage
 - DropTableStage
 - TruncateTableStage
"""

import logging

from blazingdb import exceptions
from . import base


# pragma pylint: disable=too-few-public-methods

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
                "QueryException caught when creating table %s,",
                "ignoring as it most likely means the table exists"
            ]), table)

            self.logger.debug(ex.response)


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
                "QueryException caught when dropping table %s,",
                "ignoring as it most likely means the table doesn't exist"
            ]), table)

            self.logger.debug(ex.response)


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
                "QueryException caught when truncating table %s,",
                "ignoring as it most likely means the table is already empty"
            ]), table)

            self.logger.debug(ex.response)
