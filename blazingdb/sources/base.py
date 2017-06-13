"""
Defines the base class for data sources
"""

import abc


class BaseSource(object, metaclass=abc.ABCMeta):
    """ Handles retrieving data from a given source (eg. PostgreSQL) """

    @abc.abstractmethod
    def get_tables(self):
        """ Retrieves a list of the tables in this source """

    @abc.abstractmethod
    def get_columns(self, table):
        """ Retrieves a list of columns for the given table from the source """

    @abc.abstractmethod
    def retrieve(self, table):
        """ Retrieves data for the given table from the source """
