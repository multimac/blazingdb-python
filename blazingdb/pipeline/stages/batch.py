"""
Defines the base batcher class for generating batches of data to load into BlazingDB
"""

import io
import logging

import aiofiles

from blazingdb.util import format_size, timer

from . import base
from .. import packets


# pragma pylint: disable=too-few-public-methods

class BatchStage(base.BaseStage):
    """ Handles performing requests to load data into Blazing """

    DEFAULT_LOG_INTERVAL = 10

    def __init__(self, batch_size, loop=None, **kwargs):
        super(BatchStage, self).__init__(packets.DataFilePacket, packets.DataCompletePacket)
        self.logger = logging.getLogger(__name__)

        self.generators = dict()
        self.loop = loop

        self.batch_size = batch_size
        self.log_interval = kwargs.get("log_interval", BatchStage.DEFAULT_LOG_INTERVAL)

    async def _generate_batch(self):
        def _log_progress(stream):
            read_size = format_size(stream.tell())
            limit_size = format_size(self.batch_size)

            self.logger.info("Read %s of %s byte(s)", read_size, limit_size)

        index = 0
        data_file = None
        while True:
            stream = io.StringIO(newline='')

            while stream.tell() < self.batch_size:
                log_timer = timer.RepeatedTimer(self.log_interval,
                    _log_progress, stream, loop=self.loop)

                with log_timer:
                    if data_file is None:
                        data_args = yield

                        if data_args is None:
                            if stream.tell() > 0:
                                yield stream, index

                            return

                        data_file_path, format_pkt = data_args
                        data_file = await aiofiles.open(data_file_path,
                            newline=format_pkt.line_terminator)

                    read_amount = self.batch_size - stream.tell()
                    lines = await data_file.readlines(read_amount)

                    if not lines:
                        data_file = None
                        continue

                    stream.writelines(lines)

            yield stream, index

            index += 1

    def _get_generator(self, msg_id):
        if msg_id in self.generators:
            return self.generators[msg_id]

        generator = self._generate_batch()
        generator.send(None)

        self.generators[msg_id] = generator
        return generator

    def _delete_generator(self, msg_id):
        if msg_id not in self.generators:
            return

        generator = self.generators.pop(msg_id)
        generator.close()

    async def process(self, message):
        """ Generates a series of batches from the stream """
        generator = self._get_generator(message.initial_id)
        format_pkt = message.get_packet(packets.DataFormatPacket)

        load_packets = []
        for packet in message.pop_packets(packets.DataFilePacket):
            batch = await generator.asend(packet.file_path, format_pkt)

            while batch is not None:
                stream, index = batch
                load_packet = packets.DataLoadPacket(stream, index)
                load_packets.append(load_packet)

                batch = await generator.asend(None)

        complete_packet = message.get_packet(packets.DataCompletePacket, default=None)
        if complete_packet is not None:
            batch = await generator.asend(None)

            if batch:
                stream, index = batch
                load_packet = packets.DataLoadPacket(stream, index)
                load_packets.append(load_packet)

            self._delete_generator(message.msg_id)

        if load_packets:
            await message.forward(*load_packets)
