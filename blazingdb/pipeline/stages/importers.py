"""
Defines the importing stages of the pipeline
"""

import abc
import logging
import os.path

import aiofiles
import async_timeout

from blazingdb import exceptions

from . import base
from .. import packets


class BaseImportStage(base.BaseStage):
    """ Base class for all import stages """
    def __init__(self, *packet_types, loop=None, **kwargs):
        super(BaseImportStage, self).__init__(*packet_types)

        self.loop = loop
        self.timeout = kwargs.get("timeout", None)

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

    @abc.abstractmethod
    def process(self, message):
        pass


class FileImportStage(BaseImportStage):
    """ Imports chunks of data in files """

    DEFAULT_USER_FOLDER = "data"

    def __init__(self, upload_folder, user, loop=None, **kwargs):
        super(FileImportStage, self).__init__(packets.DataFilePacket, loop=loop, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.loop = loop

        self.ignore_skipdata = kwargs.get("ignore_skipdata", False)
        self.upload_folder = os.path.join(upload_folder, user)

    def _get_import_path(self, chunk_filename):
        """ Generates a path for a given chunk of a table to be used in a query """
        relative = os.path.relpath(chunk_filename, self.upload_folder)

        if relative.startswith(os.pardir):
            raise exceptions.InvalidImportPathException(chunk_filename)

        return os.path.relpath(chunk_filename, self.upload_folder)

    async def _should_raise_warning(self, file_path, fmt):
        newline = fmt.line_terminator

        async with aiofiles.open(file_path, loop=self.loop, newline=newline) as data_file:
            return (await data_file.readline()).endswith(fmt.field_terminator)

    async def _load_chunk(self, destination, packet, table, fmt):
        """ Loads a chunk of data into Blazing """
        query_filename = self._get_import_path(packet.file_path)

        style = "infile" if not self.ignore_skipdata else "infilenoskip"
        method = "{0} {1}".format(style, query_filename)

        try:
            self.logger.info("Loading chunk %s into blazing", query_filename)
            await self._perform_request(destination, method, fmt, table)
        except exceptions.ServerImportWarning:
            if not await self._should_raise_warning(packet.file_path, fmt):
                raise

    async def process(self, message):
        import_pkt = message.get_packet(packets.ImportTablePacket)
        format_pkt = message.get_packet(packets.DataFormatPacket)
        dest_pkt = message.get_packet(packets.DestinationPacket)

        destination = dest_pkt.destination
        table = import_pkt.table

        for file_pkt in message.get_packets(packets.DataFilePacket):
            await self._load_chunk(destination, file_pkt, table, format_pkt)

        await message.forward()


class FileOutputStage(base.BaseStage):
    """ Writes a chunk out to a given file """

    DEFAULT_FILE_ENCODING = "utf-8"
    DEFAULT_FILE_EXTENSION = "dat"
    DEFAULT_USER_FOLDER = "data"

    def __init__(self, upload_folder, user, loop=None, **kwargs):
        super(FileOutputStage, self).__init__(packets.DataLoadPacket)
        self.logger = logging.getLogger(__name__)
        self.loop = loop

        self.encoding = kwargs.get("encoding", self.DEFAULT_FILE_ENCODING)
        self.file_extension = kwargs.get("file_extension", self.DEFAULT_FILE_EXTENSION)

        self.upload_folder = os.path.join(upload_folder, user)
        self.user_folder = kwargs.get("user_folder", self.DEFAULT_USER_FOLDER)

    def _open_file(self, filename):
        """ Opens the given file """
        return aiofiles.open(filename, "w", encoding=self.encoding, loop=self.loop)

    @staticmethod
    def _check_data(data, fmt):
        return data[0][-2:] == fmt.field_terminator + fmt.line_terminator

    def _get_filename(self, table, chunk):
        """ Generates a filename for the given chunk of a table """
        filename = "{0}_{1}".format(table, chunk)
        if self.file_extension is None:
            return filename

        return "{0}.{1}".format(filename, self.file_extension)

    def _get_file_path(self, table, chunk):
        """ Generates a path for a given chunk of a table to be used for writing chunks """
        file_path = self._get_filename(table, chunk)
        if self.user_folder is not None:
            file_path = os.path.join(self.user_folder, file_path)

        return os.path.join(self.upload_folder, file_path)

    async def _write_chunk(self, chunk, file_path):
        """ Writes a chunk of data to disk """
        self.logger.info("Writing chunk file: %s", file_path)

        async with self._open_file(file_path) as chunk_file:
            await chunk_file.writelines(chunk)

    async def process(self, message):
        import_pkt = message.get_packet(packets.ImportTablePacket)
        format_pkt = message.get_packet(packets.DataFormatPacket)

        for load_pkt in message.get_packets(packets.DataLoadPacket):
            chunk_filename = self._get_file_path(import_pkt.table, load_pkt.index)
            expect_warning = self._check_data(load_pkt.data, format_pkt)

            file_pkt = packets.DataFilePacket(chunk_filename, expect_warning)

            await self._write_chunk(load_pkt.data, chunk_filename)

            message.remove_packet(load_pkt)
            message.add_packet(file_pkt)

        await message.forward()


class StreamImportStage(BaseImportStage):
    """ Imports chunks of data via a stream """

    def __init__(self, loop=None, **kwargs):
        super(StreamImportStage, self).__init__(packets.DataLoadPacket, loop=loop, **kwargs)
        self.logger = logging.getLogger(__name__)

    async def _load(self, destination, table, data, fmt):
        """ Streams a chunk of data into Blazing """
        method = "stream '{0}'".format("".join(data))

        self.logger.info("Streaming %s row(s) into %s", len(data), table)
        await self._perform_request(destination, method, fmt, table)

    async def process(self, message):
        import_pkt = message.get_packet(packets.ImportTablePacket)
        format_pkt = message.get_packet(packets.DataFormatPacket)
        dest_pkt = message.get_packet(packets.DestinationPacket)

        for load_pkt in message.get_packets(packets.DataLoadPacket):
            await self._load(dest_pkt.destination, import_pkt.table, load_pkt.data, format_pkt)

        await message.forward()
