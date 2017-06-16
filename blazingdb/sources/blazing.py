"""
Defines the Postgres migrator for moving data into BlazingDB from Postgres
"""

import logging

from blazingdb.util.blazing import parse_value
from . import base


class BlazingSource(base.BaseSource):
    """ Handles connecting and retrieving data from Postgres, and loading it into BlazingDB """

    def __init__(self, connector, schema, **kwargs):
        super(BlazingSource, self).__init__()
        self.logger = logging.getLogger(__name__)

        self.connector = connector
        self.schema = schema

        self.separator = kwargs.get("separator", "$")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """ Closes the given source and cleans up the connector """
        self.connector.close()

    def get_identifier(self, table, schema=None):
        schema = self.schema if schema is None else schema
        return self.separator.join([schema, table])

    async def get_tables(self):
        """ Retrieves a list of the tables in this source """
        results = self.query("LIST TABLES")
        tables = [row[0] async for chunk in results for row in chunk]

        self.logger.debug("Retrieved %s tables from Blazing", len(tables))

        return tables

    async def get_columns(self, table):
        """ Retrieves a list of columns for the given table from the source """
        identifier = self.get_identifier(table)
        results = self.query("DESCRIBE TABLE {0}".format(identifier))

        columns = [self.Column(*row) async for chunk in results for row in chunk]

        self.logger.debug("Retrieved %s columns for table %s from Blazing", len(columns), table)

        return columns

    async def query(self, query, *args):
        """ Performs a custom query against the source """
        if args:
            raise NotImplementedError("Parameterized queries are unsupported by Blazing")

        results = await self.connector.query(query)

        # This is a hack to simulate BlazingDB's check when returning column types
        rows = results["rows"]
        types = results["columnTypes"]

        if "select" in query.lower():
            yield [parse_value(dt, val) for row in rows for dt, val in zip(types, row)]
        else:
            yield rows

    async def retrieve(self, table):
        """ Retrieves data for the given table from the source """
        columns = await self.get_columns(table)
        select_cols = ",".join(col.name for col in columns)

        results = self.query(" ".join([
            "SELECT {0}".format(select_cols),
            "FROM {0}".format(self.get_identifier(table))
        ]))

        async for chunk in results:
            yield chunk
