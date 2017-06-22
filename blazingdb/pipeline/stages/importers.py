"""
Defines the importing stages of the pipeline
"""

import abc
import logging
import os.path

import aiofiles
import async_timeout

from . import base
from .. import messages

class BaseImportStage(base.BaseStage):
    """ Base class for all import stages """
    def __init__(self, loop=None, **kwargs):
        super(BaseImportStage, self).__init__(messages.DataLoadPacket)

        self.loop = loop
        self.timeout = kwargs.get("timeout", None)

    @abc.abstractmethod
    def _load(self, destination, table, load_pkt, format_pkt):
        """ Load the given packet """

    async def _perform_request(self, destination, method, fmt, table):
        identifier = destination.get_identifier(table)
        query = " ".join([
            "load data {0} into table {1}".format(method, identifier),
            "fields terminated by '{0}'".format(fmt.field_terminator),
            "enclosed by '{0}'".format(fmt.field_wrapper),
            "lines terminated by '{0}'".format(fmt.line_terminator)
        ])

        with async_timeout.timeout(self.timeout, loop=self.loop):
            await destination.execute(query)

    def process(self, message):
        import_pkt = message.get_packet(messages.ImportTablePacket)
        format_pkt = message.get_packet(messages.DataFormatPacket)

        for load_pkt in message.get_packets(messages.DataLoadPacket):
            self._load(import_pkt.destination, import_pkt.table, load_pkt, format_pkt)


class FileImportStage(BaseImportStage):
    """ Imports chunks of data in files """

    DEFAULT_BUFFER_SIZE = -1
    DEFAULT_FILE_ENCODING = "utf-8"
    DEFAULT_FILE_EXTENSION = "dat"
    DEFAULT_USER_FOLDER = "data"

    def __init__(self, upload_folder, user, loop=None, **kwargs):
        super(FileImportStage, self).__init__(loop, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.loop = loop

        self.upload_folder = os.path.join(upload_folder, user)
        self.user_folder = kwargs.get("user_folder", self.DEFAULT_USER_FOLDER)

        self.encoding = kwargs.get("encoding", self.DEFAULT_FILE_ENCODING)
        self.file_extension = kwargs.get("file_extension", self.DEFAULT_FILE_EXTENSION)
        self.ignore_skipdata = kwargs.get("ignore_skipdata", False)

    def _open_file(self, filename):
        """ Opens the given file """
        return aiofiles.open(filename, "w", encoding=self.encoding, loop=self.loop)

    def _get_filename(self, table, chunk):
        """ Generates a filename for the given chunk of a table """
        filename = "{0}_{1}".format(table, chunk)
        if self.file_extension is None:
            return filename

        return "{0}.{1}".format(filename, self.file_extension)

    def _get_import_path(self, table, chunk):
        """ Generates a path for a given chunk of a table to be used in a query """
        filename = self._get_filename(table, chunk)
        if self.user_folder is None:
            return filename

        return os.path.join(self.user_folder, filename)

    def _get_file_path(self, table, chunk):
        """ Generates a path for a given chunk of a table to be used for writing chunks """
        import_path = self._get_import_path(table, chunk)
        return os.path.join(self.upload_folder, import_path)

    async def _write_chunk(self, chunk, table, index):
        """ Writes a chunk of data to disk """
        chunk_filename = self._get_file_path(table, index)

        self.logger.info("Writing chunk file: %s", chunk_filename)
        async with self._open_file(chunk_filename) as chunk_file:
            await chunk_file.writelines(chunk)

    async def _load_chunk(self, destination, table, index, fmt):
        """ Loads a chunk of data into Blazing """
        query_filename = self._get_import_path(table, index)

        style = "infile" if not self.ignore_skipdata else "infilenoskip"
        method = "{0} {1}".format(style, query_filename)

        self.logger.info("Loading chunk %s into blazing", query_filename)
        await self._perform_request(destination, method, fmt, table)

    async def _load(self, destination, table, load_pkt, format_pkt):
        await self._write_chunk(load_pkt.data, table, load_pkt.index)
        await self._load_chunk(destination, table, load_pkt.index, format_pkt)


class StreamImportStage(BaseImportStage):
    """ Imports chunks of data via a stream """

    def __init__(self, loop=None, **kwargs):
        super(StreamImportStage, self).__init__(loop, **kwargs)
        self.logger = logging.getLogger(__name__)

    async def _load(self, destination, table, load_pkt, format_pkt):
        """ Streams a chunk of data into Blazing """
        method = "stream '{0}'".format("".join(load_pkt.data))

        self.logger.info("Streaming %s row(s) into %s", len(load_pkt.data), table)
        await self._perform_request(destination, method, format_pkt, table)
