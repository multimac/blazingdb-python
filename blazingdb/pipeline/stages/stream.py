"""
Defines the StreamProcessor class for mapping the stream of rows from a source into a format
which can be imported into BlazingDB
"""

import asyncio
import concurrent
import functools
import operator

from blazingdb.util.blazing import DATE_FORMAT

from . import base
from .. import messages


# pylint: disable=too-few-public-methods

class StreamGenerationStage(base.BaseStage):
    """ Processes a stream of data into rows BlazingDB can import """
    def __init__(self, loop=None):
        super(StreamGenerationStage, self).__init__(messages.ImportTablePacket)
        self.loop = loop

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
        message.add_packet(messages.DataColumnsPacket(columns))

        index = 0
        tasks = list()
        async for chunk in source.retrieve(table):
            load_pkt = messages.DataLoadPacket(chunk, index)
            task = asyncio.ensure_future(message.forward(load_pkt), loop=self.loop)

            tasks.append(task)
            index += 1

        await asyncio.wait(tasks, loop=self.loop)
        await message.forward(messages.DataCompletePacket())


class StreamProcessingStage(base.BaseStage):
    """ Processes a stream into a format Blazing can import """

    DEFAULT_FIELD_TERMINATOR = "|"
    DEFAULT_FIELD_WRAPPER = "\""
    DEFAULT_LINE_TERMINATOR = "\n"

    def __init__(self, loop=None, **kwargs):
        super(StreamProcessingStage, self).__init__(
            messages.DataColumnsPacket, messages.DataLoadPacket)

        self.executor = concurrent.futures.ProcessPoolExecutor()
        self.loop = loop if loop is not None else asyncio.get_event_loop()

        self.field_terminator = kwargs.get("field_terminator", self.DEFAULT_FIELD_TERMINATOR)
        self.line_terminator = kwargs.get("line_terminator", self.DEFAULT_LINE_TERMINATOR)
        self.field_wrapper = kwargs.get("field_wrapper", self.DEFAULT_FIELD_WRAPPER)

    async def _process_in_executor(self, data, fmt, columns):
        return await self.loop.run_in_executor(self.executor, process_data, data, fmt, columns)

    async def process(self, message):
        columns = message.get_packet(messages.DataColumnsPacket).columns
        format_pkt = messages.DataFormatPacket(
            self.field_terminator, self.line_terminator, self.field_wrapper)

        for load_pkt in message.get_packets(messages.DataLoadPacket):
            processed_data = await self._process_in_executor(load_pkt.data, format_pkt, columns)
            message.update_packet(load_pkt, data=processed_data)

        message.add_packet(format_pkt)
        await message.forward()


def process_data(data, fmt, columns):
    """ Processes a given chunk using the metadata in format and columns """
    mappings = [_create_mapping(fmt, col.type) for col in columns]
    process_row = functools.partial(_process_row, fmt, mappings)

    return list(process_row, data)

def _create_mapping(fmt, datatype):
    if datatype == "date":
        return operator.methodcaller("strftime", DATE_FORMAT)
    elif datatype == "double" or datatype == "long":
        return str
    elif datatype == "string":
        return functools.partial(_wrap_field, fmt)

    raise ValueError("Given datatype ({0}) is not supported".format(datatype))

def _process_row(fmt, mappings, row):
    """ Processes a row of data into it a string to be loaded into Blazing """
    def _map_column(func, column):
        return func(column) if column is not None else ""

    fields = map(_map_column, mappings, row)
    line = fmt.field_terminator.join(fields)

    return line + fmt.line_terminator

def _wrap_field(fmt, column):
    escaped_column = column.replace(fmt.field_wrapper, "\\" + fmt.field_wrapper)
    return fmt.field_wrapper + escaped_column + fmt.field_wrapper
