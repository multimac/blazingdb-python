"""
Defines the Postgres migrator for moving data into BlazingDB from Postgres
"""

import logging

from blazingdb import exceptions

from .. import base


class PostgresSource(base.BaseSource):
    """ Handles connecting and retrieving data from Postgres, and loading it into BlazingDB """

    CURSOR_NAME = __name__
    FETCH_COUNT = 20000

    def __init__(self, pool, schema, **kwargs):
        super(PostgresSource, self).__init__()
        self.logger = logging.getLogger(__name__)

        self.pool = pool
        self.schema = schema

        self.fetch_count = kwargs.get("fetch_count", self.FETCH_COUNT)

    async def close(self):
        """ Closes the given source and cleans up the connection """
        await self.pool.close()

    def get_identifier(self, table, schema=None):
        schema = self.schema if schema is None else schema
        return ".".join([schema, table])

    async def get_tables(self):
        """ Retrieves a list of the tables in this source """
        results = self.query(" ".join([
            "SELECT DISTINCT table_name FROM information_schema.tables",
            "WHERE table_schema = '{0}' and table_type = 'BASE TABLE'".format(self.schema)
        ]))

        tables = [row[0] async for chunk in results for row in chunk]

        self.logger.debug("Retrieved %s tables from Postgres", len(tables))

        return tables

    async def get_columns(self, table):
        """ Retrieves a list of columns for the given table from the source """
        def _process_column(row):
            return base.Column(name=row[0], type=convert_datatype(row[1]), size=row[2])

        results = self.query(" ".join([
            "SELECT column_name, data_type, character_maximum_length",
            "FROM information_schema.columns",
            "WHERE table_schema = '{0}' AND table_name = '{1}'".format(self.schema, table)
        ]))

        columns = [_process_column(row) async for chunk in results for row in chunk]

        if not columns:
            raise exceptions.QueryException(None, None)

        self.logger.debug("Retrieved %s columns for table %s from Postgres", len(columns), table)

        return columns

    async def execute(self, query, *args):
        """ Executes a custom query against the source, ignoring the results """
        async with self.pool.acquire() as connection:
            await connection.execute(query, *args)

    async def query(self, query, *args):
        """ Performs a custom query against the source """
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                cursor = await connection.cursor(query, *args)

                while True:
                    chunk = await cursor.fetch(self.fetch_count)

                    if not chunk:
                        break

                    yield chunk


DATATYPE_MAP = {
    "bit": "long", "boolean": "long", "smallint": "long",
    "integer": "long", "bigint": "long",

    "double precision": "double", "money": "double",
    "numeric": "double", "real": "double",

    "character": "string",
    "character varying": "string",
    "text": "string",

    "date": "date",
    "time with time zone": "date",
    "time without time zone": "date",
    "timestamp with time zone": "date",
    "timestamp without time zone": "date"
}

def convert_datatype(datatype):
    """ Converts a PostgreSQL data type into a BlazingDB data type """
    return DATATYPE_MAP[datatype]
