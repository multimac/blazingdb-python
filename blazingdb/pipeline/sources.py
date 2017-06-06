"""
Defines a set of custom importers used by pipeline stages
"""

import logging

from ..sources import base
from ..util import gen


class FilteredSource(base.BaseSource):
    """ A custom importer which filters columns from a source """

    def __init__(self, source, columns):
        self.logger = logging.getLogger(__name__)

        self.columns = columns
        self.source = source

    def _not_filtered(self, column):
        return column["name"] not in self.columns

    def get_tables(self):
        return self.source.get_tables()

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


class LimitedSource(base.BaseSource):
    """ A custom importer which restricts the number of rows returned """

    def __init__(self, source, count):
        self.logger = logging.getLogger(__name__)

        self.source = source
        self.count = count

    def get_tables(self):
        return self.source.get_tables()

    def get_columns(self, table):
        return self.source.get_columns(table)

    def retrieve(self, table):
        stream = self.source.retrieve(table)

        with gen.GeneratorContext(stream):
            for index, row in enumerate(stream):
                if index >= self.count:
                    message = "Reached %s row limit, not returning any more rows"

                    self.logger.debug(message, self.count)
                    break

                yield row
