"""
Defines the base batcher class for generating batches of data to load into BlazingDB
"""

import abc
import logging

from blazingdb.util import timer
from . import base


# pragma pylint: disable=too-few-public-methods

class BaseBatchStage(base.BaseStage, metaclass=abc.ABCMeta):
    """ Handles performing requests to load data into Blazing """

    DEFAULT_LOG_INTERVAL = 10

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.log_interval = kwargs.get("log_interval", self.DEFAULT_LOG_INTERVAL)

    @abc.abstractmethod
    def _init_batch(self):
        """ Initializes a batch, returning an object to be passed as data to other calls """

    @abc.abstractmethod
    def _update_batch(self, data, row):
        """ Called to update the data object when a row is added to a batch """

    @abc.abstractmethod
    def _reached_limit(self, data):
        """ Called to check if a batch has reached the limit and should be returned """

    @abc.abstractmethod
    def _log_progress(self, data):
        """ Called periodically to monitor the progress of a batch """

    def _generate_batch(self, data):
        batch_data = self._init_batch()
        stream = data["stream"]

        with timer.RepeatedTimer(10, self._log_progress, batch_data):
            if data["last_row"] is not None:
                self._update_batch(batch_data, data["last_row"])
                yield data["last_row"]

            data["last_row"] = None
            for row in stream:
                if self._reached_limit(batch_data):
                    data["last_row"] = row
                    break

                self._update_batch(batch_data, row)
                yield row

        self._log_progress(batch_data)

    async def process(self, step, data):
        """ Generates a series of batches from the stream """

        batch_data = {
            "stream": data["stream"],
            "last_row": None
        }

        index = 0
        while True:
            batch = self._generate_batch(batch_data)

            async for item in step({"stream": batch, "index": index}):
                yield item

            if batch_data["last_row"] is None:
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

    @staticmethod
    def _format_size(size, suffix="B"):
        format_str = "%.1f%s%s"
        for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
            if abs(size) < 1024:
                return format_str % (size, unit, suffix)

            size /= 1024

        return format_str % (size, "Yi", suffix)

    def _init_batch(self):
        return {
            "batch_length": 0,
            "byte_count": 0,
            "last_count": -1
        }

    def _update_batch(self, data, row):
        encoded_row = row.encode(self.encoding)

        data["batch_length"] += 1
        data["byte_count"] += len(encoded_row)

    def _reached_limit(self, data):
        return data["byte_count"] >= self.size

    def _log_progress(self, data):
        if data["byte_count"] == data["last_count"]:
            return

        self.logger.info(
            "Read %s of %s (%s row(s)) from the stream",
            self._format_size(data["byte_count"]),
            self._format_size(self.size),
            data["batch_length"]
        )

        data["last_count"] = data["byte_count"]


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

    def _update_batch(self, data, row):
        data["batch_length"] += 1

    def _reached_limit(self, data):
        return data["batch_length"] >= self.count

    def _log_progress(self, data):
        if data["batch_length"] == data["last_count"]:
            return

        self.logger.info(
            "Read %s of %s row(s) from the stream",
            data["batch_length"], data["last_count"]
        )

        data["last_count"] = data["batch_length"]
