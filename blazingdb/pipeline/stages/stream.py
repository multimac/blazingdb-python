"""
Defines the StreamProcessor class for mapping the stream of rows from a source into a format
which can be imported into BlazingDB
"""

import functools
import operator

from blazingdb.util.blazing import DATE_FORMAT

from . import base
from .. import messages


# pylint: disable=too-few-public-methods

class StreamGenerationStage(base.BaseStage):
    """ Processes a stream of data into rows BlazingDB can import """
    def __init__(self):
        super(StreamGenerationStage, self).__init__(messages.ImportTablePacket)

    @staticmethod
    def _create_stream(data):
        source = data["source"]
        table = data["src_table"]

        return source.retrieve(table)

    async def process(self, message):
        import_pkt = message.get_packet(messages.ImportTablePacket)

        source = import_pkt.source
        table = import_pkt.table

        columns = await source.get_columns(table)

        index = 0
        async for chunk in source.retrieve(table):
            await message.forward(messages.DataLoadPacket(chunk, columns, index))
            index += 1

        await message.forward(messages.DataCompletePacket())


class StreamProcessingStage(base.BaseStage):
    """ Processes a stream into a format Blazing can import """

    DEFAULT_FIELD_TERMINATOR = "|"
    DEFAULT_FIELD_WRAPPER = "\""
    DEFAULT_LINE_TERMINATOR = "\n"

    def __init__(self, **kwargs):
        super(StreamProcessingStage, self).__init__(messages.DataLoadPacket)
        self.field_terminator = kwargs.get("field_terminator", self.DEFAULT_FIELD_TERMINATOR)
        self.line_terminator = kwargs.get("line_terminator", self.DEFAULT_LINE_TERMINATOR)
        self.field_wrapper = kwargs.get("field_wrapper", self.DEFAULT_FIELD_WRAPPER)

    def _create_mapping(self, datatype):
        if datatype == "date":
            return operator.methodcaller("strftime", DATE_FORMAT)
        elif datatype == "double" or datatype == "long":
            return str
        elif datatype == "string":
            return functools.partial(self._wrap_field)

        raise ValueError("Given datatype is not supported")

    def _process_row(self, mappings, row):
        """ Processes a row of data into it a string to be loaded into Blazing """
        def _map_column(func, column):
            return func(column) if column is not None else ""

        fields = map(_map_column, mappings, row)
        line = self.field_terminator.join(fields)

        return line + self.line_terminator

    def _process(self, packet):
        mappings = map(self._create_mapping, [col.type for col in packet.columns])
        process_row = functools.partial(self._process_row, mappings)

        return map(process_row, packet.data)

    def _wrap_field(self, column):
        escaped_column = column.replace(self.field_wrapper, "\\" + self.field_wrapper)
        return self.field_wrapper + escaped_column + self.field_wrapper

    async def process(self, message):
        for load_pkt in message.get_packets(messages.DataLoadPacket):
            message.update_packet(load_pkt, data=self._process(load_pkt))

        message.add_packet(messages.DataFormatPacket(
            self.field_terminator, self.line_terminator, self.field_wrapper
        ))

        await message.forward()
