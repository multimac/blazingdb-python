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
import string

from blazingdb import sources

from . import base
from .. import packets


# pragma pylint: disable=too-few-public-methods

class ChainedSource(sources.BaseSource):
    """ A custom source used to override methods on the given source """

    def __init__(self, source):
        self.source = source

    def _parse_row(self, columns, row):
        # pragma pylint: disable=protected-access
        return self.source._parse_row(columns, row)

    def get_identifier(self, table, schema=None):
        return self.source.get_identifier(table, schema)

    async def get_tables(self):
        return self.source.get_tables()

    async def get_columns(self, table):
        return self.source.get_columns(table)

    async def query(self, query, *args):
        return self.source.query(query, *args)

    async def retrieve(self, table):
        return self.source.retrieve(table)


class AlteredStreamSource(ChainedSource, metaclass=abc.ABCMeta):
    """ A custom source used to override the stream of rows from the given source """

    @abc.abstractmethod
    async def _alter_chunk(self, table, chunk):
        pass

    async def retrieve(self, table):
        stream = self.source.retrieve(table)

        async for chunk in stream:
            yield await self._alter_chunk(table, chunk)


class FilterColumnsStage(base.BaseStage):
    """ Filters the given columns from the imported data """

    def __init__(self, tables):
        super(FilterColumnsStage, self).__init__(packets.ImportTablePacket)
        self.logger = logging.getLogger(__name__)
        self.tables = tables

    async def process(self, message):
        """ Replaces the stream with one which filters the columns """
        packet = message.get_packet(packets.ImportTablePacket)
        ignored_columns = self.tables.get(packet.table, [])

        self.logger.info(
            "Filtering %s columns from %s%s", len(ignored_columns), packet.table,
            " ({0})".format(", ".join(ignored_columns)) if ignored_columns else "")

        message.update(packet, source=FilteredSource(packet.source, ignored_columns))

        await message.forward()


class FilteredSource(ChainedSource):
    """ A custom importer which filters columns from a source """

    def __init__(self, source, columns):
        super(FilteredSource, self).__init__(source)

        self.logger = logging.getLogger(__name__)
        self.columns = columns

    def _not_filtered(self, column):
        return column.name not in self.columns

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

        self.logger.debug("Generated %s row segments for table %s", len(slices), table)

        def _filter_row(row):
            filtered_row = []

            for row_slice in slices:
                filtered_row.extend(row[row_slice])

            yield filtered_row


        results = self.source.retrieve(table)
        async for chunk in results:
            if len(slices) == 1:
                yield chunk
                continue

            yield map(_filter_row, chunk)


class JumbleDataStage(base.BaseStage):
    """ Jumbles the data being loaded to obfuscate any sensitive information """

    def __init__(self):
        super(JumbleDataStage, self).__init__(packets.ImportTablePacket)

    async def process(self, message):
        packet = message.get_packet(packets.ImportTablePacket)
        message.update(packet, source=JumbledSource(message.source))

        await message.forward()


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

    def _get_random_func(self, col):
        if col.type == "date":
            func = self._random_date
        elif col.type == "double":
            func = self._random_double
        elif col.type == "long":
            func = self._random_long
        elif col.type == "string":
            func = self._random_string(col.size)

        return func

    async def _alter_chunk(self, table, chunk):
        columns = await self.source.get_columns(table)

        types = [col.type for col in columns]
        type_funcs = [self._get_random_func(t) for t in types]

        def _process_row(_):
            return [func() for func in type_funcs]

        return [_process_row(row) for row in chunk]


class LimitImportStage(base.BaseStage):
    """ Limits the number of rows imported from the source """

    def __init__(self, count):
        super(LimitImportStage, self).__init__(packets.ImportTablePacket)
        self.count = count

    async def process(self, message):
        """ Replaces the source with one which limits the number of rows returned """
        packet = message.get_packet(packets.ImportTablePacket)
        message.update(packet, source=LimitedSource(message.source, self.count))

        await message.forward()


class LimitedSource(ChainedSource):
    """ A custom importer which restricts the number of rows returned """

    def __init__(self, source, count):
        super(LimitedSource, self).__init__(source)

        self.logger = logging.getLogger(__name__)
        self.count = count

    async def retrieve(self, table):
        returned = 0
        async for chunk in self.source.retrieve(table):
            chunk_length = len(chunk)

            if returned + chunk_length > self.count:
                yielded = self.count - returned
                yield chunk[:yielded]
            else:
                yielded = chunk_length
                yield chunk

            returned += yielded

            if returned >= self.count:
                break
