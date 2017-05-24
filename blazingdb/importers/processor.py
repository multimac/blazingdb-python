"""
Defines the StreamProcessor for mapping the stream of rows from a source into a format which
can be imported into BlazingDB
"""

import logging

from . import base
from ..util import timer


class StreamProcessor(object):  # pylint: disable=too-many-instance-attributes
    """ Processes a stream of data into rows BlazingDB can import """

    DEFAULT_LOG_INTERVAL = 10

    def __init__(self, stream, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.last_row = None
        self.stream = stream

        self.encoding = kwargs.get("encoding", base.DEFAULT_FILE_ENCODING)
        self.field_terminator = kwargs.get("field_terminator", base.DEFAULT_FIELD_TERMINATOR)
        self.field_wrapper = kwargs.get("field_wrapper", base.DEFAULT_FIELD_WRAPPER)
        self.line_terminator = kwargs.get("line_terminator", base.DEFAULT_LINE_TERMINATOR)

        self.log_interval = kwargs.get("log_interval", self.DEFAULT_LOG_INTERVAL)

    def _wrap_field(self, column):
        return self.field_wrapper + column + self.field_wrapper

    def _process_column(self, column):
        if column is None:
            return ""
        elif isinstance(column, str):
            return self._wrap_field(column)

        try:
            return column.strftime("%Y-%m-%d")
        except AttributeError:
            pass

        return str(column)

    def _process_row(self, row):
        """ Processes a row of data into it a string to be loaded into Blazing """
        fields = map(self._process_column, row)
        line = self.field_terminator.join(fields)

        return line + self.line_terminator

    def _load_row(self):
        """ Reads a single row from the stream and sets it as self.last_row """
        self.last_row = None

        if self.stream is None:
            raise StopIteration()

        self.last_row = next(self.stream)

    def _build_batch(self, stop_check):
        """ Reads rows from the stream until stop_check returns True """
        if self.last_row is None:
            self._load_row()

        while not stop_check(self.last_row):
            yield self.last_row

            self._load_row()

    def _read_bytes(self, size):
        """ Reads rows from the stream until the next row would exceed the given size (in bytes) """
        self.logger.debug("Reading %s bytes from the stream", size)

        byte_count = 0
        row_count = 0
        def _log_progress():
            nonlocal row_count
            self.logger.debug(
                "Read %s of %s bytes (%s rows) from the stream",
                byte_count, size, row_count
            )

        def _stop_check(row):
            nonlocal byte_count, row_count

            processed_row = self._process_row(row)
            raw_row = processed_row.encode(self.encoding)

            if byte_count + len(raw_row) > size:
                return True

            byte_count += len(raw_row)
            row_count += 1
            return False

        with timer.RepeatedTimer(10, _log_progress):
            yield from self._build_batch(_stop_check)

        self.logger.debug("Read %s row(s) (%s bytes) from the stream", row_count, byte_count)

    def _read_rows(self, count):
        """ Reads the given number of rows from the stream """
        self.logger.debug("Reading %s row(s) from the stream", count)

        row_count = 0
        def _log_progress():
            nonlocal row_count
            self.logger.debug(
                "Read %s of %s rows from the stream",
                row_count, count
            )

        def _stop_check(_):
            nonlocal row_count

            if row_count >= count:
                return True

            row_count += 1
            return False

        with timer.RepeatedTimer(self.log_interval, _log_progress):
            yield from self._build_batch(_stop_check)

        self.logger.debug("Read %s row(s) from the stream", row_count)

    def batch_bytes(self, size):
        """ Reads rows from the stream until the next row would exceed the given size (in bytes) """
        while True:
            batch = self._read_bytes(size)
            yield map(self._process_row, batch)

            if self.last_row is None:
                break

    def batch_rows(self, count):
        """ Reads the given number of rows from the stream """
        while True:
            batch = self._read_rows(count)
            yield map(self._process_row, batch)

            if self.last_row is None:
                break
