"""
Defines the base importer class for loading data into BlazingDB
"""

import abc
import async_timeout

from ..pipeline import system

DEFAULT_FILE_ENCODING = "utf-8"
DEFAULT_FIELD_TERMINATOR = "|"
DEFAULT_FIELD_WRAPPER = "\""
DEFAULT_LINE_TERMINATOR = "\n"

class BaseImporter(object, metaclass=abc.ABCMeta):  # pylint: disable=too-few-public-methods
    """ Handles performing requests to load data into Blazing """

    def __init__(self, loop=None, **kwargs):
        self.loop = loop

        self.field_terminator = kwargs.get("field_terminator", DEFAULT_FIELD_TERMINATOR)
        self.field_wrapper = kwargs.get("field_wrapper", DEFAULT_FIELD_WRAPPER)
        self.line_terminator = kwargs.get("line_terminator", DEFAULT_LINE_TERMINATOR)

        self.pipeline = kwargs.get("pipeline", system.System())
        self.timeout = kwargs.get("timeout", None)

    async def _pipeline_request(self, data):
        connector = data["connector"]
        method = data["method"]
        table = data["dest_table"]

        query = " ".join([
            "load data {0} into table {1}".format(method, table),
            "fields terminated by '{0}'".format(self.field_terminator),
            "enclosed by '{0}'".format(self.field_wrapper),
            "lines terminated by '{0}'".format(self.line_terminator)
        ])

        with async_timeout.timeout(self.timeout, loop=self.loop):
            await connector.query(query)

    async def _perform_request(self, connector, method, table):
        """ Runs a query to load the data into Blazing using the given method """
        import_data = {
            "connector": connector,
            "dest_table": table,
            "importer": self,
            "method": method
        }

        async with self.pipeline.process(import_data):
            await self._pipeline_request(import_data)

    @abc.abstractmethod
    async def load(self, data):
        """ Reads from the stream and imports the data into the table of the given name """
        pass
