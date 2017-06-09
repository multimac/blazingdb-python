"""
Defines the RowBatcher class for generating batches containing a given number of rows
"""

import logging

from . import base

class RowBatcher(base.BaseBatcher):  # pylint: disable=too-few-public-methods
    """ Handles performing requests to load data into Blazing """

    def __init__(self, count, **kwargs):
        super(RowBatcher, self).__init__(**kwargs)

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
