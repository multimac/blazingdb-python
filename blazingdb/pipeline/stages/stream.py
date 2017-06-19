"""
Defines the StreamProcessor class for mapping the stream of rows from a source into a format
which can be imported into BlazingDB
"""

import functools

from blazingdb import importers
from blazingdb.util.blazing import DATE_FORMAT
from . import base


# pylint: disable=too-few-public-methods

class StreamGenerationStage(base.BaseStage):
    """ Processes a stream of data into rows BlazingDB can import """

    DEFAULT_FIELD_TERMINATOR = "|"
    DEFAULT_FIELD_WRAPPER = "\""
    DEFAULT_LINE_TERMINATOR = "\n"

    def __init__(self, **kwargs):
        super(StreamGenerationStage, self).__init__()
        self.field_terminator = kwargs.get("field_terminator", self.DEFAULT_FIELD_TERMINATOR)
        self.line_terminator = kwargs.get("line_terminator", self.DEFAULT_LINE_TERMINATOR)
        self.field_wrapper = kwargs.get("field_wrapper", self.DEFAULT_FIELD_WRAPPER)

    @staticmethod
    def _create_stream(data):
        source = data["source"]
        table = data["src_table"]

        return source.retrieve(table)

    @staticmethod
    def _map_date(column):
        return column.strftime(DATE_FORMAT)

    def _wrap_field(self, row_format, column):
        return row_format.field_wrapper + column + row_format.field_wrapper

    def _create_mapping(self, row_format, datatype):
        if datatype == "date":
            return self._map_date
        elif datatype == "double" or datatype == "long":
            return str
        elif datatype == "string":
            return functools.partial(self._wrap_field, row_format)

        raise ValueError("Given datatype is not supported")

    def _process_row(self, row_format, mappings, row):
        """ Processes a row of data into it a string to be loaded into Blazing """
        def _map_column(func, column):
            return func(column) if column is not None else ""

        fields = map(_map_column, mappings, row)
        line = row_format.field_terminator.join(fields)

        return line + row_format.line_terminator

    async def process(self, step, data):
        source = data["source"]
        table = data["src_table"]

        row_format = importers.RowFormat(
            field_terminator=self.field_terminator,
            line_terminator=self.line_terminator,
            field_wrapper=self.field_wrapper
        )

        columns = source.get_columns(table)
        stream = source.retrieve(table)

        mappings = (self._create_mapping(row_format, col.type) for col in columns)
        process_row = functools.partial(self._process_row, row_format, mappings)

        index = 0
        async for chunk in stream:
            next_data = {
                "format": row_format, "index": index, "source": None,
                "stream": map(process_row, chunk)
            }

            async for item in step(next_data):
                yield item
