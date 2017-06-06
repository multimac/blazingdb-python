"""
Defines the base importer class for loading data into BlazingDB
"""

import abc
import async_timeout

from . import processor
from ..pipeline import system

class BaseImporter(object, metaclass=abc.ABCMeta):  # pylint: disable=too-few-public-methods
    """ Handles performing requests to load data into Blazing """

    def __init__(self, loop=None, **kwargs):
        self.loop = loop

        self.args = kwargs

        self.pipeline = kwargs.get("pipeline", system.System())
        self.timeout = kwargs.get("timeout", None)

    def _create_stream(self, data):
        source = data["source"]
        table = data["src_table"]

        stream = source.retrieve(table)

        return processor.StreamProcessor(stream, **self.args)

    async def _pipeline_request(self, data):
        connector = data["connector"]
        method = data["method"]
        stream_processor = data["processor"]
        table = data["dest_table"]

        query = " ".join([
            "load data {0} into table {1}".format(method, table),
            "fields terminated by '{0}'".format(stream_processor.field_terminator),
            "enclosed by '{0}'".format(stream_processor.field_wrapper),
            "lines terminated by '{0}'".format(stream_processor.line_terminator)
        ])

        with async_timeout.timeout(self.timeout, loop=self.loop):
            await connector.query(query)

    async def _perform_request(self, connector, method, stream_processor, table):
        """ Runs a query to load the data into Blazing using the given method """

        import_data = {
            "connector": connector,
            "dest_table": table,
            "importer": self,
            "method": method,
            "processor": stream_processor
        }

        async with self.pipeline.process(import_data):
            await self._pipeline_request(import_data)

    @abc.abstractmethod
    async def load(self, data):
        """ Reads from the stream and imports the data into the table of the given name """
        pass
