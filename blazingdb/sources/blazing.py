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

    def get_tables(self):
        """ Retrieves a list of the tables in this source """
        results = self.query("LIST TABLES")
        tables = [row[0] for row in results]

        self.logger.debug("Retrieved %s tables from Blazing", len(tables))

        return tables

    def get_columns(self, table):
        """ Retrieves a list of columns for the given table from the source """
        identifier = self.separator.join([self.schema, table])
        results = self.query("DESCRIBE TABLE {0}".format(identifier))

        columns = []
        for row in results:
            column = self.Column(**row)
            columns.append(column)

        self.logger.debug("Retrieved %s columns for table %s from Blazing", len(columns), table)

        return columns

    def query(self, query, *args):
        """ Performs a custom query against the source """
        if args:
            raise NotImplementedError("Parameterized queries are unsupported by Blazing")

        yield from self.connector.query(query)["rows"]

    def retrieve(self, table):
        """ Retrieves data for the given table from the source """
        columns = self.get_columns(table)
        identifier = self.separator.join([self.schema, table])

        results = self.query(" ".join([
            "SELECT {0}".format(",".join(col.name for col in columns)),
            "FROM {0}".format(identifier)
        ]))

        for row in results:
            yield [parse_value(col.type, val) for col, val in zip(columns, row)]
