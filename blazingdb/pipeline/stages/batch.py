"""
Defines the base batcher class for generating batches of data to load into BlazingDB
"""

import gc
import logging

import pandas

from . import base
from .. import packets


# pragma pylint: disable=too-few-public-methods

class BatchStage(base.BaseStage):
    """ Handles performing requests to load data into Blazing """

    DEFAULT_LOG_INTERVAL = 10

    def __init__(self, batch_size):
        super(BatchStage, self).__init__(packets.DataFramePacket, packets.DataCompletePacket)
        self.logger = logging.getLogger(__name__)

        self.batch_size = batch_size
        self.generators = dict()

    async def shutdown(self):
        for gen in self.generators.values():
            gen.close()

    def _generate_batch(self):
        frame_data = yield

        index = 0
        while True:
            while True:
                memory_usage = frame_data.memory_usage().sum()
                row_count = frame_data.shape[0]

                if memory_usage < self.batch_size:
                    break

                batch_ratio = self.batch_size / memory_usage
                batch_rows = int(batch_ratio * row_count)

                batch_frame = frame_data.iloc[:batch_rows].copy()
                frame_data = frame_data.iloc[batch_rows:]

                yield batch_frame, index

                index += 1

            # Free up memory by shrinking the frame_data and collecting the old frame
            frame_data = frame_data.copy()
            gc.collect()

            next_frame = yield

            if next_frame is None:
                break

            frame_data = pandas.concat(
                [frame_data, next_frame])

        yield frame_data, index

    def _get_generator(self, msg_id):
        if msg_id in self.generators:
            return self.generators[msg_id]

        generator = self._generate_batch()
        generator.send(None)

        self.generators[msg_id] = generator
        return generator

    def _delete_generator(self, msg_id):
        generator = self.generators.pop(msg_id)
        generator.close()

    async def process(self, message):
        """ Generates a series of batches from the stream """
        generator = self._get_generator(message.initial_id)

        frame_packets = []
        for packet in message.pop_packets(packets.DataFramePacket):
            batch = generator.send(packet.frame)

            while batch is not None:
                frame, index = batch

                frame_packet = packets.DataFramePacket(frame, index)
                frame_packets.append(frame_packet)

                batch = generator.send(None)

        complete_packet = message.get_packet(
            packets.DataCompletePacket, default=None)

        if complete_packet is not None:
            batch = generator.send(None)

            if batch:
                frame, index = batch

                frame_packet = packets.DataFramePacket(frame, index)
                frame_packets.append(frame_packet)

            self._delete_generator(message.initial_id)

        if frame_packets:
            self.logger.info("Created %s segments of data from message %s",
                len(frame_packets), message.msg_id)

            await message.forward(*frame_packets)
