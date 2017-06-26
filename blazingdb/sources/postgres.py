"""
Defines the Postgres migrator for moving data into BlazingDB from Postgres
"""

import logging

from blazingdb.util.blazing import parse_value

from . import base


class PostgresPoolConnection(object):  # pylint: disable=too-few-public-methods
    """ Handles retrieving and returning connections in a pool """

    def __init__(self, pool):
        self.pool = pool
        self.connection = None

    def __enter__(self):
        self.connection = self.pool.getconn()
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.putconn(self.connection)
        self.connection = None


class PostgresSource(base.BaseSource):
    """ Handles connecting and retrieving data from Postgres, and loading it into BlazingDB """

    CURSOR_NAME = __name__
    FETCH_COUNT = 50000

    def __init__(self, pool, schema, **kwargs):
        super(PostgresSource, self).__init__()
        self.logger = logging.getLogger(__name__)

        self.pool = pool
        self.schema = schema

        self.fetch_count = kwargs.get("fetch_count", self.FETCH_COUNT)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """ Closes the given source and cleans up the connection """
        self.pool.closeall()

    def _create_cursor(self, connection):
        cursor = connection.cursor(self.CURSOR_NAME)
        cursor.itersize = self.fetch_count

        return cursor

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
        results = self.query(" ".join([
            "SELECT column_name, data_type, character_maximum_length",
            "FROM information_schema.columns",
            "WHERE table_schema = '{0}' AND table_name = '{1}'".format(self.schema, table)
        ]))


        def _process_column(row):
            return self.Column(name=row[0], type=convert_datatype(row[1]), size=row[2])

        columns = [_process_column(row) async for chunk in results for row in chunk]

        self.logger.debug("Retrieved %s columns for table %s from Postgres", len(columns), table)

        return columns

    async def query(self, query, *args):
        """ Performs a custom query against the source """
        with PostgresPoolConnection(self.pool) as connection:
            with self._create_cursor(connection) as cursor:
                cursor.execute(query, *args)

                while True:
                    chunk = cursor.fetchmany(self.fetch_count)

                    if not chunk:
                        break

                    yield chunk

    async def retrieve(self, table):
        """ Retrieves data for the given table from the source """
        columns = await self.get_columns(table)
        select_cols = ",".join(column.name for column in columns)

        results = self.query(" ".join([
            "SELECT {0}".format(select_cols),
            "FROM {0}".format(self.get_identifier(table))
        ]))

        def _process_row(row):
            return [parse_value(col.type, val) for col, val in zip(columns, row)]

        async for chunk in results:
            yield map(_process_row, chunk)


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
