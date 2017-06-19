"""
Defines the base batcher class for generating batches of data to load into BlazingDB
"""

import abc
import collections
import logging
import math
import operator

from blazingdb.util import format_size, timer
from . import base


# pragma pylint: disable=too-few-public-methods

class BaseBatchStage(base.BaseStage, metaclass=abc.ABCMeta):
    """ Handles performing requests to load data into Blazing """

    DEFAULT_LOG_INTERVAL = 10

    def __init__(self, **kwargs):
        super(BaseBatchStage, self).__init__()
        self.log_interval = kwargs.get("log_interval", self.DEFAULT_LOG_INTERVAL)

    @abc.abstractmethod
    def _init_batch(self):
        """ Initializes a batch, returning an object to be passed as data to other calls """

    @abc.abstractmethod
    def _process_chunk(self, data, batch, chunk):
        """ Called to process rows in a chunk into a batch """

    @abc.abstractmethod
    def _log_complete(self, data):
        """ Called upon completion of a batch to perform a final log message """

    @abc.abstractmethod
    def _log_progress(self, data):
        """ Called periodically to monitor the progress of a batch """

    async def _generate_batch(self, stream, last_chunk):
        batch = []
        batch_data = self._init_batch()

        with timer.RepeatedTimer(10, self._log_progress, batch_data):
            if last_chunk is not None:
                remaining = self._process_chunk(batch_data, batch, last_chunk)

                if remaining is not None:
                    self._log_complete(batch_data)
                    return (batch, remaining)

            async for chunk in stream:
                remaining = self._process_chunk(batch_data, batch, chunk)

                if remaining is not None:
                    self._log_complete(batch_data)
                    return (batch, remaining)

        self._log_complete(batch_data)
        return (batch, None)

    async def process(self, step, data):
        """ Generates a series of batches from the stream """

        index = 0
        last_chunk = None
        stream = data["stream"]

        while True:
            batch, last_chunk = await self._generate_batch(stream, last_chunk)

            async for item in step({"stream": batch, "index": index}):
                yield item

            if last_chunk is None:
                break

            index += 1


class ByteBatchStage(BaseBatchStage):
    """ Handles performing requests to load data into Blazing """

    DEFAULT_ENCODING = "utf-8"

    def __init__(self, size, **kwargs):
        super(ByteBatchStage, self).__init__(**kwargs)
        self.logger = logging.getLogger(__name__)

        self.encoding = kwargs.get("encoding", self.DEFAULT_ENCODING)
        self.size = size

    @abc.abstractmethod
    def _process_chunk(self, data, batch, chunk):
        """ Called to process rows in a chunk into a batch """

    def _init_batch(self):
        return {
            "batch_length": 0,
            "byte_count": 0,
            "last_count": -1
        }

    def _log_complete(self, data):
        self.logger.info(
            "Read %s (%s row(s)) from the stream",
            format_size(data["byte_count"]),
            data["batch_length"]
        )

    def _log_progress(self, data):
        if data["byte_count"] == data["last_count"]:
            return

        self.logger.info(
            "Read %s of %s (%s row(s)) from the stream",
            format_size(data["byte_count"]),
            format_size(self.size),
            data["batch_length"]
        )

        data["last_count"] = data["byte_count"]


class PreciseByteBatchStage(ByteBatchStage):
    """ Handles performing requests to load data into Blazing """

    def __init__(self, size, **kwargs):
        super(PreciseByteBatchStage, self).__init__(size, **kwargs)

    def _update_batch(self, data, row):
        encoded_row = row.encode(self.encoding)

        data["batch_length"] += 1
        data["byte_count"] += len(encoded_row)

    def _reached_limit(self, data):
        return data["byte_count"] >= self.size

    def _process_chunk(self, data, batch, chunk):
        chunk = collections.deque(chunk)

        while chunk:
            row = chunk.popleft()
            batch.append(row)

            self._update_batch(data, row)

            if self._reached_limit(data):
                return chunk

        return None


class RoughByteBatchStage(ByteBatchStage):
    """ Handles performing requests to load data into Blazing """

    DEFAULT_ROWS_AVERAGE = 50

    def __init__(self, size, **kwargs):
        super(RoughByteBatchStage, self).__init__(size, **kwargs)
        self.rows_in_average = kwargs.get("rows_in_average", self.DEFAULT_ROWS_AVERAGE)

    def _determine_row_size(self, chunk):
        encoded = map(operator.methodcaller("encode", self.encoding), chunk)
        return sum(map(len, encoded)) / len(chunk)

    def _process_chunk(self, data, batch, chunk):
        chunk = list(chunk)

        rows_in_average = chunk[:self.rows_in_average]
        avg_size = self._determine_row_size(rows_in_average)

        difference = self.size - data["byte_count"]
        rough_size = avg_size * len(chunk)

        if rough_size <= difference:
            data["batch_length"] += len(chunk)
            data["byte_count"] += rough_size

            batch.extend(chunk)
            return None

        proportion = difference / rough_size
        row_count = math.floor(len(chunk) * proportion)

        data["batch_length"] += row_count
        data["byte_count"] += avg_size * row_count

        batch.extend(chunk[:row_count])
        return chunk[row_count:]


class RowBatchStage(BaseBatchStage):
    """ Handles performing requests to load data into Blazing """

    def __init__(self, count, **kwargs):
        super(RowBatchStage, self).__init__(**kwargs)
        self.logger = logging.getLogger(__name__)

        self.count = count

    def _init_batch(self):
        return {
            "batch_length": 0,
            "last_count": -1
        }

    def _process_chunk(self, data, batch, chunk):
        chunk = list(chunk)

        difference = self.count - data["batch_length"]
        chunk_length = len(chunk)

        if chunk_length <= difference:
            data["batch_length"] += chunk_length
            batch.extend(chunk)

            return None

        data["batch_length"] += difference
        batch.extend(chunk[:difference])

        return chunk[difference:]

    def _log_complete(self, data):
        self.logger.info(
            "Read %s row(s) from the stream",
            data["batch_length"]
        )

    def _log_progress(self, data):
        if data["batch_length"] == data["last_count"]:
            return

        self.logger.info(
            "Read %s of %s row(s) from the stream",
            data["batch_length"], data["last_count"]
        )

        data["last_count"] = data["batch_length"]
