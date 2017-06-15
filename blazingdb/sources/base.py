"""
Defines the base class for data sources
"""

import abc
from collections import namedtuple


class BaseSource(object, metaclass=abc.ABCMeta):
    """ Handles retrieving data from a given source (eg. PostgreSQL) """

    Column = namedtuple("Column", ["name", "type", "size"])

    @abc.abstractmethod
    def get_identifier(self, table, schema=None):
        """ Creates an identifier for the given schema and table """

    @abc.abstractmethod
    async def get_tables(self):
        """ Retrieves a list of the tables in this source """

    @abc.abstractmethod
    async def get_columns(self, table):
        """ Retrieves a list of columns for the given table from the source """

    @abc.abstractmethod
    async def query(self, query, *args):
        """ Performs a custom query against the source """

    @abc.abstractmethod
    async def retrieve(self, table):
        """ Retrieves data for the given table from the source """

    async def execute(self, query, *args):
        """ Executes a custom query against the source, ignoring the results """
        results = self.query(query, *args)

        async for _ in results:
            return
