"""
Defines the Importer classes (Stream and Chunking) which handles chunking and
loading data into BlazingDB
"""

import abc
import itertools
import logging
import queue
import threading

from os import path


DEFAULT_FILE_ENCODING = "utf-8"
DEFAULT_FIELD_TERMINATOR = "|"
DEFAULT_FIELD_WRAPPER = "\""
DEFAULT_LINE_TERMINATOR = "\n"


class StreamProcessor(object):
    """ Processes a stream of data into rows BlazingDB can import """

    def __init__(self, stream, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.last_row = None
        self.stream = stream

        self.encoding = kwargs.get("encoding", DEFAULT_FILE_ENCODING)
        self.field_terminator = kwargs.get("field_terminator", DEFAULT_FIELD_TERMINATOR)
        self.field_wrapper = kwargs.get("field_wrapper", DEFAULT_FIELD_WRAPPER)
        self.line_terminator = kwargs.get("line_terminator", DEFAULT_LINE_TERMINATOR)

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
        return self.field_terminator.join(fields) + self.line_terminator

    def _load_row(self):
        if self.stream is None:
            return

        try:
            self.last_row = next(self.stream)
        except StopIteration:
            self.last_row = None
            self.stream = None

    def _build_batch(self, stop_check):
        if self.last_row is None:
            self._load_row()

        batch = []
        while self.last_row is not None:
            if stop_check(self.last_row):
                break

            batch.append(self.last_row)
            self._load_row()

        return batch

    def _read_bytes(self, size):
        """ Reads rows from the stream until the next row would exceed the given size (in bytes) """
        self.logger.debug("Reading %s bytes from the stream", size)

        byte_count = 0
        row_count = 0
        def stop_check(row):
            nonlocal byte_count, row_count

            processed_row = self._process_row(row)
            raw_row = processed_row.encode(self.encoding)

            if byte_count + len(raw_row) > size:
                return True

            byte_count += len(raw_row)
            row_count += 1
            return False

        batch = self._build_batch(stop_check)

        self.logger.debug("Read %s row(s) (%s bytes) from the stream", row_count, byte_count)

        return batch

    def _read_rows(self, count):
        """ Reads the given number of rows from the stream """
        self.logger.debug("Reading %s row(s) from the stream", count)

        row_count = 0
        def stop_check(row):
            nonlocal row_count

            if row_count >= count:
                return True

            row_count += 1
            return False

        batch = self._build_batch(stop_check)

        self.logger.debug("Read %s row(s) from the stream", row_count)

        return batch

    def read_bytes(self, size):
        """ Reads rows from the stream until the next row would exceed the given size (in bytes) """
        return list(map(self._process_row, self._read_bytes(size)))

    def read_rows(self, count):
        """ Reads the given number of rows from the stream """
        return list(map(self._process_row, self._read_rows(count)))


class BlazingImporter(object):  # pylint: disable=too-few-public-methods
    """ Handles performing requests to load data into Blazing """

    def __init__(self, **kwargs):
        self.field_terminator = kwargs.get("field_terminator", DEFAULT_FIELD_TERMINATOR)
        self.field_wrapper = kwargs.get("field_wrapper", DEFAULT_FIELD_WRAPPER)
        self.line_terminator = kwargs.get("line_terminator", DEFAULT_LINE_TERMINATOR)

    def _perform_request(self, connector, method, table):
        """ Runs a query to load the data into Blazing using the given method """
        query = " ".join([
            "load data {0} into table {1}".format(method, table),
            "fields terminated by '{0}'".format(self.field_terminator),
            "enclosed by '{0}'".format(self.field_wrapper),
            "lines terminated by '{0}'".format(self.line_terminator)
        ])

        connector.query(query, auto_connect=True)

    @abc.abstractmethod
    def load(self, connector, data):
        """ Reads from the stream and imports the data into the table of the given name """
        pass


class StreamImporter(BlazingImporter):  # pylint: disable=too-few-public-methods
    """ Handles the loading of data into Blazing using a stream """

    DEFAULT_CHUNK_SIZE = 1048576

    def __init__(self, **kwargs):
        super(StreamImporter, self).__init__(**kwargs)
        self.logger = logging.getLogger(__name__)

        self.processor_args = kwargs
        self.chunk_size = kwargs.get("chunk_size", self.DEFAULT_CHUNK_SIZE)

    def _stream_chunk(self, connector, data, table):
        """ Streams a chunk of data into Blazing """
        method = "stream '{0}'".format("".join(data))

        self.logger.info("Streaming %s row(s) into %s", len(rows), table)
        self._perform_request(connector, method, table)

    def load(self, connector, data):
        """ Reads from the stream and imports the data into the table of the given name """
        processor = StreamProcessor(data["stream"], **self.processor_args)

        while True:
            chunk_data = processor.read_bytes(self.chunk_size)
            if len(chunk_data) == 0:
                break

            self._stream_chunk(connector, chunk_data, data["dest_table"])


class ChunkingImporter(BlazingImporter):  # pylint: disable=too-few-public-methods
    """ Handles the loading of data into Blazing using flat files """

    DEFAULT_CHUNK_ROWS = 100000
    DEFAULT_FILE_EXTENSION = "dat"

    def __init__(self, upload_folder, user, user_folder, **kwargs):
        super(ChunkingImporter, self).__init__(**kwargs)
        self.logger = logging.getLogger(__name__)

        self.processor_args = kwargs

        self.upload_folder = path.join(upload_folder, user)
        self.user_folder = user_folder

        self.encoding = kwargs.get("encoding", DEFAULT_FILE_ENCODING)
        self.file_extension = kwargs.get("file_extension", self.DEFAULT_FILE_EXTENSION)
        self.row_count = kwargs.get("row_count", self.DEFAULT_CHUNK_ROWS)

    def _get_filename(self, table, chunk):
        filename = "{0}_{1}".format(table, chunk)
        if self.file_extension is None:
            return filename

        return "{0}.{1}".format(filename, self.file_extension)

    def _get_file_path(self, table, chunk):
        """ Generates a path for a given chunk of a table to be used for writing chunks """
        import_path = self._get_import_path(table, chunk)
        return path.join(self.upload_folder, import_path)

    def _get_import_path(self, table, chunk):
        """ Generates a path for a given chunk of a table to be used in a query """
        filename = self._get_filename(table, chunk)
        if self.user_folder is None:
            return filename

        return path.join(self.user_folder, filename)

    def _write_chunk(self, data, table, chunk):
        """ Writes a chunk of data to disk """
        chunk_filename = self._get_file_path(table, chunk)
        chunk_data = "".join(data)

        self.logger.info("Writing chunk file (%s bytes): %s", len(chunk_data), chunk_filename)

        with open(chunk_filename, "w", encoding=self.encoding) as chunk_file:
            chunk_file.write(chunk_data)

    def _load_chunk(self, connector, table, chunk):
        """ Loads a chunk of data into Blazing """
        query_filename = self._get_import_path(table, chunk)
        method = "infile {0}".format(query_filename)

        self.logger.info("Loading chunk %s into blazing", query_filename)
        self._perform_request(connector, method, table)

    def _load_chunk_loop(self, connector, queue):
        """ Processes chunks to be loaded in the given queue """
        self.logger.debug("Beginning chunk loading thread...")

        while True:
            filename_parts = queue.get()
            if filename_parts is None:
                break

            self._load_chunk(connector, *filename_parts)

    def load(self, connector, data):
        """ Reads from the stream and imports the data into the table of the given name """
        processor = StreamProcessor(data["stream"], **self.processor_args)

        load_queue = queue.Queue()
        load_thread = threading.Thread(
            target=self._load_chunk_loop,
            args=(connector, load_queue)
            daemon=False
        )

        counter = 0
        load_thread.start()
        while True:
            chunk_data = processor.read_rows(self.row_count)
            if len(chunk_data) == 0:
                load_queue.put(None)
                break

            filename_parts = (data["dest_table"], counter)
            self._write_chunk(chunk_data, *filename_parts)

            load_queue.put(filename_parts)
            counter += 1

        self.logger.info("Waiting for chunk loading to complete...")
        load_thread.join()
