"""
Defines the base batcher class for generating batches of data to load into BlazingDB
"""

import abc
import logging

from ...util import timer

class BaseBatcher(object, metaclass=abc.ABCMeta):  # pylint: disable=too-few-public-methods
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

    def batch(self, stream):
        """ Generates a series of batches from the stream """

        def _add_row(batch, data, row):
            self._update_batch(data, row)
            batch.append(row)

        last_row = None
        while True:
            batch = []
            data = self._init_batch()

            with timer.RepeatedTimer(10, self._log_progress, batch, data):
                if last_row is not None:
                    _add_row(batch, data, last_row)

                last_row = None
                for row in stream:
                    if not batch:
                        _add_row(batch, data, row)
                        continue

                    if self._reached_limit(data):
                        last_row = row
                        break

                    _add_row(batch, data, row)

            yield batch

            if last_row is None:
                break
