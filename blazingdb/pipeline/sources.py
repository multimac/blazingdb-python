"""
Defines a set of custom importers used by pipeline stages
"""

import abc
import datetime
import logging
import random
import re
import string

from ..sources import base
from ..util import gen


class ChainedSource(base.BaseSource):
    """ A custom source used to override methods on the given source """

    def __init__(self, source):
        self.source = source

    def get_tables(self):
        return self.source.get_tables()

    def get_columns(self, table):
        return self.source.get_columns(table)

    def retrieve(self, table):
        return self.source.retrieve(table)


class AlteredStreamSource(ChainedSource, metaclass=abc.ABCMeta):
    """ A custom source used to override the stream of rows from the given source """

    @abc.abstractmethod
    def _alter_stream(self, table, stream):
        pass

    def retrieve(self, table):
        stream = self.source.retrieve(table)

        with gen.GeneratorContext(stream):
            yield from self._alter_stream(table, stream)


class FilteredSource(ChainedSource):
    """ A custom importer which filters columns from a source """

    def __init__(self, source, columns):
        super(FilteredSource, self).__init__(source)

        self.logger = logging.getLogger(__name__)
        self.columns = columns

    def _not_filtered(self, column):
        return column["name"] not in self.columns

    def get_columns(self, table):
        columns = self.source.get_columns(table)

        return list(filter(self._not_filtered, columns))

    def retrieve(self, table):
        current_start = None
        slices = []

        columns = self.get_columns(table)
        for index, column in enumerate(columns):
            if self._not_filtered(column):
                if current_start is None:
                    current_start = index

                continue

            if current_start is not None:
                slices.append(slice(current_start, index))
                current_start = None

        if current_start is not None:
            slices.append(slice(current_start, None))

        self.logger.debug(
            "Generated %s row segments for table %s",
            len(slices), table
        )

        for row in self.source.retrieve(table):
            filtered_row = []

            for row_slice in slices:
                filtered_row.extend(row[row_slice])

            yield filtered_row


class JumbledSource(AlteredStreamSource):
    """ Jumbles the data being retrieved from the source to obscure sensitive information """

    def __init__(self, source):
        super(JumbledSource, self).__init__(source)

        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _random_date(near_date, variance=90):
        offset = random.randint(0, variance)
        delta = datetime.timedelta(days=offset)

        return near_date + delta

    @staticmethod
    def _random_double(length):
        decimal = random.randint(1, length - 1)

        rand_digit = lambda: random.choice(string.digits)
        rand_segment = lambda x: [rand_digit() for _ in range(x)]

        return float(".".join([rand_segment(decimal), rand_segment(length - decimal)]))

    @staticmethod
    def _random_long(length):
        rand_digit = lambda: random.choice(string.digits)
        return int("".join(rand_digit() for _ in range(length)))

    @staticmethod
    def _random_string(length=10):
        options = " " + string.ascii_lowercase
        jumbled = "".join(random.choice(options) for _ in range(length))

        return jumbled.title()

    def _alter_stream(self, table, stream):
        types = self.source.get_columns(table)

        for row in stream:
            jumbled = []

            for i, item in enumerate(row):
                col_type = types[i]["type"]
                if col_type == "date":
                    near_date = item if item is not None else datetime.date.today()
                    jumbled.append(self._random_date(near_date))
                else:
                    length = len(str(item)) if item is not None else 10
                    if col_type == "double":
                        jumbled.append(self._random_double(length))
                    elif col_type == "long":
                        jumbled.append(self._random_long(length))
                    elif re.fullmatch("string([0-9]+)", col_type):
                        jumbled.append(self._random_string(length))

            self.logger.debug("%s ==> %s", row, jumbled)

            yield jumbled


class LimitedSource(AlteredStreamSource):
    """ A custom importer which restricts the number of rows returned """

    def __init__(self, source, count):
        super(LimitedSource, self).__init__(source)

        self.logger = logging.getLogger(__name__)
        self.count = count

    def _alter_stream(self, table, stream):
        for index, row in enumerate(stream):
            if index >= self.count:
                message = "Reached %s row limit, not returning any more rows"

                self.logger.debug(message, self.count)
                break

            yield row
