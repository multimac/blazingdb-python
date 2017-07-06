"""
Defines the StreamProcessor class for mapping the stream of rows from a source into a format
which can be imported into BlazingDB
"""

import asyncio
import functools
import logging
import operator
import signal

from blazingdb import processor
from blazingdb.util.blazing import DATE_FORMAT
from blazingdb.util import process

from . import base
from .. import packets


# pylint: disable=too-few-public-methods

class StreamGenerationStage(base.BaseStage):
    """ Processes a stream of data into rows BlazingDB can import """
    def __init__(self, loop=None):
        super(StreamGenerationStage, self).__init__(packets.ImportTablePacket)
        self.loop = loop

    async def process(self, message):
        import_pkt = message.get_packet(packets.ImportTablePacket)

        source = import_pkt.source
        table = import_pkt.table

        columns = await source.get_columns(table)
        message.add_packet(packets.DataColumnsPacket(columns))

        index = 0
        handles = []
        async for chunk in source.retrieve(table):
            packet = packets.DataLoadPacket(chunk, index)
            handle = await message.forward(packet, track_following=True)

            while len(handles) >= 5:
                _, pending = await asyncio.wait(
                    handles, loop=self.loop,
                    return_when=asyncio.FIRST_COMPLETED)

                handles = list(pending)

            handles.append(handle)
            index += 1

        await asyncio.wait(handles, loop=self.loop)
        await message.forward(packets.DataCompletePacket())


class StreamProcessingStage(base.BaseStage):
    """ Processes a stream into a format Blazing can import """

    DEFAULT_FIELD_TERMINATOR = "|"
    DEFAULT_FIELD_WRAPPER = "\""
    DEFAULT_LINE_TERMINATOR = "\n"

    def __init__(self, loop=None, **kwargs):
        super(StreamProcessingStage, self).__init__(
            packets.DataColumnsPacket, packets.DataLoadPacket)

        self.logger = logging.getLogger(__name__)

        self.executor = process.ProcessPoolExecutor(_quiet_sigint)
        self.loop = loop if loop is not None else asyncio.get_event_loop()

        self.field_terminator = kwargs.get("field_terminator", self.DEFAULT_FIELD_TERMINATOR)
        self.line_terminator = kwargs.get("line_terminator", self.DEFAULT_LINE_TERMINATOR)
        self.field_wrapper = kwargs.get("field_wrapper", self.DEFAULT_FIELD_WRAPPER)

    async def shutdown(self):
        self.executor.shutdown(wait=True)

    async def _process_in_executor(self, data, fmt, columns):
        return await self.loop.run_in_executor(self.executor, process_data, data, fmt, columns)

    async def process(self, message):
        columns = message.get_packet(packets.DataColumnsPacket).columns
        format_pkt = packets.DataFormatPacket(
            self.field_terminator, self.line_terminator, self.field_wrapper)

        for load_pkt in message.get_packets(packets.DataLoadPacket):
            processed_data = await self._process_in_executor(load_pkt.data, format_pkt, columns)
            message.update_packet(load_pkt, data=processed_data)

        message.add_packet(format_pkt)
        await message.forward()


def process_data(data, fmt, columns):
    """ Processes a given chunk using the metadata in format and columns """
    mappings = [_create_mapping(fmt, col.type) for col in columns]
    process_row = functools.partial(_process_row, fmt, mappings)

    return list(map(process_row, data))

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

def _quiet_sigint():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def _wrap_field(fmt, column):
    escaped_column = column.replace(fmt.field_wrapper, "\\" + fmt.field_wrapper)
    return fmt.field_wrapper + escaped_column + fmt.field_wrapper
