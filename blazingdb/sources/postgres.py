"""
Defines the Postgres migrator for moving data into BlazingDB from Postgres
"""

import logging

from . import base


class PostgresSource(base.BaseSource):
    """ Handles connecting and retrieving data from Postgres, and loading it into BlazingDB """

    CURSOR_NAME = __name__
    FETCH_COUNT = 50000

    def __init__(self, connection, schema, **kwargs):
        super(PostgresSource, self).__init__()
        self.logger = logging.getLogger(__name__)

        self.connection = connection
        self.schema = schema

        self.fetch_count = kwargs.get("fetch_count", self.FETCH_COUNT)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """ Closes the given source and cleans up the connection """
        self.connection.close()

    def _create_cursor(self):
        cursor = self.connection.cursor(self.CURSOR_NAME)
        cursor.itersize = self.fetch_count

        return cursor

    def _process_results(self, cursor, quiet=True):
        while True:
            chunk = cursor.fetchmany(self.fetch_count)

            if not quiet:
                self.logger.debug("Retrieved chunk of %s rows from Postgres", len(chunk))

            if len(chunk) == 0:
                raise StopIteration

            yield from chunk

    def get_tables(self):
        """ Retrieves a list of the tables in this source """
        with self._create_cursor() as cursor:
            cursor.execute(" ".join([
                "SELECT DISTINCT table_name FROM information_schema.tables",
                "WHERE table_schema = '{0}' and table_type = 'BASE TABLE'".format(self.schema)
            ]))

            tables = [row[0] for row in self._process_results(cursor)]

        self.logger.debug("Retrieved %s tables from Postgres", len(tables))

        return tables

    def get_columns(self, table):
        """ Retrieves a list of columns for the given table from the source """
        with self._create_cursor() as cursor:
            cursor.execute(" ".join([
                "SELECT column_name, data_type, character_maximum_length",
                "FROM information_schema.columns",
                "WHERE table_schema = '{0}' AND table_name = '{1}'".format(self.schema, table)
            ]))

            columns = []
            for row in self._process_results(cursor):
                datatype = convert_datatype(row[1], row[2])
                columns.append({"name": row[0], "type": datatype})

        self.logger.debug("Retrieved %s columns for table %s from Postgres", len(columns), table)

        return columns

    def retrieve(self, table):
        """ Retrieves data for the given table from the source """
        columns = self.get_columns(table)

        with self._create_cursor() as cursor:
            cursor.execute(" ".join([
                "SELECT {0}".format(",".join(column["name"] for column in columns)),
                "FROM {0}.{1}".format(self.schema, table)
            ]))

            yield from self._process_results(cursor, quiet=False)


DATATYPE_MAP = {
    'bit': 'long', 'boolean': 'long', 'smallint': 'long',
    'integer': 'long', 'bigint': 'long',

    'double precision': 'double', 'money': 'double',
    'numeric': 'double', 'real': 'double',

    'character': 'string({0})',
    'character varying': 'string({0})',
    'text': 'string({0})',

    'date': 'date',
    'time with time zone': 'date',
    'time without time zone': 'date',
    'timestamp with time zone': 'date',
    'timestamp without time zone': 'date'
}


def convert_datatype(datatype, size):
    """ Converts a PostgreSQL data type into a BlazingDB data type """
    return DATATYPE_MAP.get(datatype).format(size)
