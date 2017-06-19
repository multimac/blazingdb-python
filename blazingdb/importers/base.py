"""
Defines the base importer class for loading data into BlazingDB
"""

import abc
from collections import namedtuple

import async_timeout

RowFormat = namedtuple("RowFormat", ["field_terminator", "field_wrapper", "line_terminator"])

class BaseImporter(object, metaclass=abc.ABCMeta):  # pylint: disable=too-few-public-methods
    """ Handles performing requests to load data into Blazing """

    def __init__(self, loop=None, **kwargs):
        self.loop = loop
        self.timeout = kwargs.get("timeout", None)

    async def _perform_request(self, destination, method, fmt, table):
        identifier = destination.get_identifier(table)
        query = " ".join([
            "load data {0} into table {1}".format(method, identifier),
            "fields terminated by '{0}'".format(fmt.field_terminator),
            "enclosed by '{0}'".format(fmt.field_wrapper),
            "lines terminated by '{0}'".format(fmt.line_terminator)
        ])

        with async_timeout.timeout(self.timeout, loop=self.loop):
            await destination.execute(query)

    @abc.abstractmethod
    async def load(self, data):
        """ Processes an import request into batches and loads each batch into BlazingDB """
