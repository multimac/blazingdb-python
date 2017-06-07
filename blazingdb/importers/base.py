"""
Defines the base importer class for loading data into BlazingDB
"""

import abc
import async_timeout

from . import processor
from ..pipeline import system

class BaseImporter(object, metaclass=abc.ABCMeta):  # pylint: disable=too-few-public-methods
    """ Handles performing requests to load data into Blazing """

    def __init__(self, batcher, loop=None, **kwargs):
        self.batcher = batcher
        self.loop = loop

        self.processor = processor.StreamProcessor(**kwargs)

        self.pipeline = kwargs.get("pipeline", system.System())
        self.timeout = kwargs.get("timeout", None)

    @staticmethod
    def _create_stream(data):
        source = data["source"]
        table = data["src_table"]

        return source.retrieve(table)

    async def _perform_request(self, data):
        connector = data["connector"]
        method = data["method"]
        table = data["dest_table"]

        query = " ".join([
            "load data {0} into table {1}".format(method, table),
            "fields terminated by '{0}'".format(self.processor.field_terminator),
            "enclosed by '{0}'".format(self.processor.field_wrapper),
            "lines terminated by '{0}'".format(self.processor.line_terminator)
        ])

        with async_timeout.timeout(self.timeout, loop=self.loop):
            await connector.query(query)

    async def _load_data(self, connector, method, table):
        """ Runs a query to load the data into Blazing using the given method """

        import_data = {
            "connector": connector,
            "dest_table": table,
            "importer": self,
            "method": method
        }

        async with self.pipeline.process(import_data):
            await self._perform_request(import_data)

    @abc.abstractmethod
    def _init_load(self, data):
        """ Initializes a data load, returning an object to be passed to _load_batch """

    @abc.abstractmethod
    async def _load_batch(self, data, batch):
        """ Called to load the given batch of data into BlazingDB """

    async def load(self, data):
        """ Processes an import request into batches and loads each batch into BlazingDB """
        stream = self._create_stream(data)
        processed = self.processor.process(stream)

        load_data = self._init_load(data)
        for batch in self.batcher.batch(processed):
            await self._load_batch(load_data, batch)
