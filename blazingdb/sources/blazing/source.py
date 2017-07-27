"""
Defines the Postgres migrator for moving data into BlazingDB from Postgres
"""

import logging

import pandas

from blazingdb import exceptions

from .. import base


class BlazingSource(base.BaseSource):
    """ Handles connecting and retrieving data from Postgres, and loading it into BlazingDB """

    def __init__(self, connector, schema, **kwargs):
        super(BlazingSource, self).__init__()
        self.logger = logging.getLogger(__name__)

        self.separator = kwargs.get("separator", "$")
        self.connector = connector
        self.schema = schema

    async def close(self):
        """ Closes the given source and cleans up the connector """
        self.connector.close()

    def get_identifier(self, table, schema=None):
        schema = self.schema if schema is None else schema
        return self.separator.join([schema, table])

    async def get_tables(self):
        """ Retrieves a list of the tables in this source """
        results = self.query("LIST TABLES")
        tables = [table async for frame in results for table in frame.iloc[:, 0]]

        self.logger.debug("Retrieved %s tables from Blazing", len(tables))

        return tables

    async def get_columns(self, table):
        """ Retrieves a list of columns for the given table from the source """
        identifier = self.get_identifier(table)
        results = self.query("DESCRIBE TABLE {0}".format(identifier))

        def _process_column(row):
            return base.Column(name=row[0], type=convert_datatype(row[1]), size=row[2])

        columns = [_process_column(row[1]) async for frame in results for row in frame.iterrows()]

        if not columns:
            raise exceptions.QueryException(None, None)

        self.logger.debug("Retrieved %s columns for table %s from Blazing", len(columns), table)

        return columns

    async def execute(self, query, *args):
        """ Executes a custom query against the source, ignoring the results """
        results = self.query(query, *args)

        async for _ in results:
            return

    async def query(self, query, *args):
        """ Performs a custom query against the source """
        if args:
            raise NotImplementedError("Parameterized queries are unsupported by Blazing")

        results = await self.connector.query(query)
        frame = pandas.DataFrame(results["rows"])

        if results["columnTypes"] is not None:
            types = [convert_datatype(t) for t in results["columnTypes"]]

            for i, column_type in enumerate(types):
                if column_type == "str":
                    continue

                frame[i] = frame[i].astype(column_type)

        yield frame


DATATYPE_MAP = {
    "bool": "bool", "date": "date",

    "float": "float", "double": "float",

    "char": "long", "short": "long",
    "int": "long", "long": "long",

    "string": "str"
}

def convert_datatype(datatype):
    """ Converts a BlazingDB data type into a Python data type """
    return DATATYPE_MAP[datatype]
