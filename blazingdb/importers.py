"""
Defines the Importer classes (Stream and Chunking) which handles chunking and
loading data into BlazingDB
"""

import abc
import itertools
import logging

from os import path


class StreamProcessor(object):
    """ Processes a stream of data into rows BlazingDB can import """

    def __init__(self, stream, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.last_row = None
        self.stream = stream

        self.encoding = kwargs.get("encoding", "utf-8")
        self.field_terminator = kwargs.get("field_terminator", "|")
        self.field_wrapper = kwargs.get("field_wrapper", "\"")
        self.line_terminator = kwargs.get("line_terminator", "\n")

    def _process_row(self, row):
        """ Processes a row of data into it a string to be loaded into Blazing """
        fields = [str.format("{0}{1}{0}", self.field_wrapper, f) for f in row]
        return self.field_terminator.join(fields) + self.line_terminator

    def _read_bytes(self, size):
        """ Reads rows from the stream until the next row would exceed the given size (in bytes) """
        if self.last_row is None:
            self.last_row = next(self.stream)

        byte_count = 0
        while True:
            row_size = len(self._process_row(self.last_row).encode(self.encoding))
            if byte_count + row_size > size:
                break

            yield self.last_row
            self.last_row = next(self.stream)

    def _read_rows(self, count):
        """ Reads the given number of rows from the stream """
        if self.last_row is not None:
            stream_slice = itertools.islice(self.stream, count - 1)
            return itertools.chain([self.last_row], stream_slice)

        return itertools.islice(self.stream, count)

    def read_bytes(self, size):
        """ Reads rows from the stream until the next row would exceed the given size (in bytes) """
        return map(self._process_row, self._read_bytes(size))

    def read_rows(self, count):
        """ Reads the given number of rows from the stream """
        return map(self._process_row, self._read_rows(count))


class BlazingImporter(object):  # pylint: disable=too-few-public-methods
    """ Handles performing requests to load data into Blazing """

    def __init__(self, connector, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.connector = connector

        self.field_terminator = kwargs.get("field_terminator", "|")
        self.field_wrapper = kwargs.get("field_wrapper", "\"")
        self.line_terminator = kwargs.get("line_terminator", "\n")

    def _perform_request(self, method, table):
        """ Runs a query to load the data into Blazing using the given method """
        query = " ".join([
            str.format("load data {0} into table {1}", method, table),
            str.format("fields terminated by '{0}'", self.field_terminator),
            str.format("enclosed by '{0}'", self.field_wrapper),
            str.format("lines terminated by '{0}'", self.line_terminator)
        ])

        self.connector.query(query, auto_connect=True)

    @abc.abstractmethod
    def load(self, table, stream):
        """ Reads from the stream and imports the data into the table of the given name """
        pass


class StreamImporter(BlazingImporter):  # pylint: disable=too-few-public-methods
    """ Handles the loading of data into Blazing using a stream """

    def __init__(self, connector, **kwargs):
        super(StreamImporter, self).__init__(connector, kwargs)
        self.logger = logging.getLogger(__name__)

        self.processor_args = kwargs
        self.chunk_size = kwargs.get("chunk_size", 1048576)

    def _stream_chunk(self, data, table):
        """ Streams a chunk of data into Blazing """
        method = str.join("stream '{0}'", "".join(data))
        self._perform_request(method, table)

    def load(self, table, stream):
        """ Reads from the stream and imports the data into the table of the given name """
        processor = StreamProcessor(stream, **self.processor_args)

        while True:
            chunk_data = processor.read_bytes(self.chunk_size)
            if not chunk_data:
                break

            self._stream_chunk(chunk_data, table)


class ChunkingImporter(BlazingImporter):  # pylint: disable=too-few-public-methods
    """ Handles the loading of data into Blazing using flat files """

    def __init__(self, connector, target_path, **kwargs):
        super(ChunkingImporter, self).__init__(connector, **kwargs)
        self.logger = logging.getLogger(__name__)

        self.processor_args = kwargs
        self.target_path = target_path

        self.chunk_size = kwargs.get("chunk_size", 1048576)
        self.file_extension = kwargs.get("file_extension", "dat")

    def _get_file_path(self, table, chunk):
        """ Generates a path for a given chunk of a table """
        filename = str.format("{0}_{1}.{2}", table, chunk, self.file_extension)
        return path.join(self.target_path, filename)

    def _load_chunk(self, data, table, i):
        """ Loads a chunk of data into Blazing """
        chunk_filename = self._get_file_path(table, i)

        with open(chunk_filename, "w", encoding="utf-8") as chunk_file:
            for line in data:
                chunk_file.write(line)

        method = str.join("infile {0}", chunk_filename)
        self._perform_request(method, table)

    def load(self, table, stream):
        """ Reads from the stream and imports the data into the table of the given name """
        processor = StreamProcessor(stream, **self.processor_args)

        counter = 0
        while True:
            chunk_data = processor.read_bytes(self.chunk_size)
            if not chunk_data:
                break

            self._load_chunk(chunk_data, table, counter)

            counter += 1
