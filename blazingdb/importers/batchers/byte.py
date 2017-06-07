"""
Defines the ByteBatcher class for generating batches of a given size
"""

import logging

from . import base

class ByteBatcher(base.BaseBatcher):  # pylint: disable=too-few-public-methods
    """ Handles performing requests to load data into Blazing """

    DEFAULT_FILE_ENCODING = "utf-8"

    def __init__(self, size, **kwargs):
        super(ByteBatcher, self).__init__(**kwargs)

        self.logger = logging.getLogger(__name__)

        self.encoding = kwargs.get("encoding", self.DEFAULT_FILE_ENCODING)
        self.size = size

    def _init_batch(self):
        return {"batch_length": 0, "byte_count": 0, "last_count": 0}

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
            "Read %s of %s bytes (%s row(s)) from the stream",
            data["byte_count"], self.size, data["batch_length"]
        )

        data["last_count"] = data["byte_count"]