"""
Defines the StreamProcessor class for mapping the stream of rows from a source into a format
which can be imported into BlazingDB
"""

import logging

from blazingdb import importers
from . import base


# pylint: disable=too-few-public-methods

class StreamGenerationStage(base.BaseStage):
    """ Processes a stream of data into rows BlazingDB can import """

    DEFAULT_FIELD_TERMINATOR = "|"
    DEFAULT_FIELD_WRAPPER = "\""
    DEFAULT_LINE_TERMINATOR = "\n"

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.field_terminator = kwargs.get("field_terminator", self.DEFAULT_FIELD_TERMINATOR)
        self.line_terminator = kwargs.get("line_terminator", self.DEFAULT_LINE_TERMINATOR)
        self.field_wrapper = kwargs.get("field_wrapper", self.DEFAULT_FIELD_WRAPPER)

    @staticmethod
    def _create_stream(data):
        source = data["source"]
        table = data["src_table"]

        return source.retrieve(table)

    def _wrap_field(self, column):
        return self.field_wrapper + column + self.field_wrapper

    def _process_column(self, column):
        if column is None:
            return ""
        elif isinstance(column, str):
            return self._wrap_field(column)

        try:
            return column.strftime("%Y-%m-%d")
        except AttributeError:
            pass

        return str(column)

    def _process_row(self, row):
        """ Processes a row of data into it a string to be loaded into Blazing """
        fields = map(self._process_column, row)
        line = self.field_terminator.join(fields)

        return line + self.line_terminator

    def _process_stream(self, stream):
        """ Processes a stream of rows into lines of an import into BlazingDB """
        for row in stream:
            fields = map(self._process_column, row)
            line = self.field_terminator.join(fields)

            yield line + self.line_terminator

    async def process(self, step, data):
        stream = self._create_stream(data)
        processed = self._process_stream(stream)

        generator = step({
            "format": importers.RowFormat(
                field_terminator=self.field_terminator,
                line_terminator=self.line_terminator,
                field_wrapper=self.field_wrapper
            ),

            "index": 0,
            "source": None,
            "stream": processed
        })

        async for item in generator:
            yield item
