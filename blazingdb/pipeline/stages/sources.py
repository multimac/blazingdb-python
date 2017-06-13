"""
Defines a series of pipeline stages for affecting the sources, including:
 - FilterColumnsStage
 - JumbleDataStage
 - LimitImportStage
"""

import abc
import datetime
import logging
import random
import re
import string

from blazingdb import sources
from blazingdb.util import aenumerate, gen
from . import base


# pragma pylint: disable=too-few-public-methods

class ChainedSource(sources.BaseSource):
    """ A custom source used to override methods on the given source """

    def __init__(self, source):
        self.source = source

    async def get_tables(self):
        return self.source.get_tables()

    async def get_columns(self, table):
        return self.source.get_columns(table)

    async def retrieve(self, table):
        return self.source.retrieve(table)


class AlteredStreamSource(ChainedSource, metaclass=abc.ABCMeta):
    """ A custom source used to override the stream of rows from the given source """

    @abc.abstractmethod
    def _alter_stream(self, table, stream):
        pass

    async def retrieve(self, table):
        stream = self.source.retrieve(table)

        with gen.GeneratorContext(stream):
            async for row in self._alter_stream(table, stream):
                yield row


class FilterColumnsStage(base.BaseStage):
    """ Filters the given columns from the imported data """

    def __init__(self, tables):
        self.logger = logging.getLogger(__name__)
        self.tables = tables

    async def before(self, data):
        """ Replaces the stream with one which filters the columns """
        ignored_columns = self.tables.get(data["src_table"], [])

        self.logger.info(
            "Filtering %s columns from %s%s", len(ignored_columns), data["src_table"],
            " ({0})".format(", ".join(ignored_columns)) if ignored_columns else ""
        )

        data["source"] = FilteredSource(data["source"], ignored_columns)


class FilteredSource(ChainedSource):
    """ A custom importer which filters columns from a source """

    def __init__(self, source, columns):
        super(FilteredSource, self).__init__(source)

        self.logger = logging.getLogger(__name__)
        self.columns = columns

    def _not_filtered(self, column):
        return column["name"] not in self.columns

    async def get_columns(self, table):
        columns = await self.source.get_columns(table)

        return list(filter(self._not_filtered, columns))

    async def retrieve(self, table):
        current_start = None
        slices = []

        columns = await self.get_columns(table)
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

        async for row in self.source.retrieve(table):
            filtered_row = []

            for row_slice in slices:
                filtered_row.extend(row[row_slice])

            yield tuple(filtered_row)


class JumbleDataStage(base.BaseStage):
    """ Jumbles the data being loaded to obfuscate any sensitive information """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    async def before(self, data):
        data["source"] = JumbledSource(data["source"])


class JumbledSource(AlteredStreamSource):
    """ Jumbles the data being retrieved from the source to obscure sensitive information """

    def __init__(self, source):
        super(JumbledSource, self).__init__(source)

        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _random_date(start=datetime.date(2000, 1, 1), end=datetime.date.today()):
        offset = random.randint(0, (end - start).days)
        delta = datetime.timedelta(days=offset)

        return start + delta

    @staticmethod
    def _random_double(length=8):
        return random.uniform(1, (10 ** length) - 1)

    @staticmethod
    def _random_long(length=8):
        return random.randint(1, (10 ** length) - 1)

    @staticmethod
    def _random_string(length=12):
        rand_char = lambda: random.choice(string.ascii_lowercase + " ")
        return "".join(rand_char() for _ in range(length)).title()

    def _get_random_func(self, col_type):
        if col_type == "date":
            func = self._random_date
        elif col_type == "double":
            func = self._random_double
        elif col_type == "long":
            func = self._random_long
        else:
            match = re.fullmatch(r"string\([0-9]+\)", col_type)

            if match:
                length = min(int(match.group(1)), 12)
                func = self._random_string(length)

        return func

    async def _alter_stream(self, table, stream):
        types = [t["type"] for t in await self.source.get_columns(table)]
        type_funcs = [self._get_random_func(t) for t in types]

        async for _ in stream:
            yield (func() for func in type_funcs)


class LimitImportStage(base.BaseStage):
    """ Limits the number of rows imported from the source """

    def __init__(self, count):
        self.count = count

    async def before(self, data):
        """ Replaces the source with one which limits the number of rows returned """
        data["source"] = LimitedSource(data["source"], self.count)


class LimitedSource(AlteredStreamSource):
    """ A custom importer which restricts the number of rows returned """

    def __init__(self, source, count):
        super(LimitedSource, self).__init__(source)

        self.logger = logging.getLogger(__name__)
        self.count = count

    async def _alter_stream(self, table, stream):
        async for index, row in aenumerate(stream):
            if index >= self.count:
                message = "Reached %s row limit, not returning any more rows"

                self.logger.debug(message, self.count)
                break

            yield row
