"""
Defines the importing stages of the pipeline
"""

import abc
import csv
import logging
import os.path

import aiofiles
import async_timeout

from blazingdb import exceptions
from blazingdb.util import process

from . import base
from .. import packets


def write_frame(frame, file_path, format_pkt):
    """ Writes a data frame to disk """
    frame.to_csv(file_path, sep=format_pkt.field_terminator,
        line_terminator=format_pkt.line_terminator, quotechar=format_pkt.field_wrapper,
        quoting=csv.QUOTE_NONNUMERIC, doublequote=False, escapechar="\\",
        header=False, index=False, date_format=format_pkt.date_format)


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

    DEFAULT_DATE_FORMAT = "%Y-%m-%d"
    DEFAULT_FIELD_TERMINATOR = "|"
    DEFAULT_LINE_TERMINATOR = "\n"
    DEFAULT_FIELD_WRAPPER = "\""

    def __init__(self, upload_folder, user, loop=None, **kwargs):
        super(FileOutputStage, self).__init__(packets.DataFramePacket)
        self.logger = logging.getLogger(__name__)
        self.loop = loop

        self.executor = process.ProcessPoolExecutor(process.quiet_sigint)

        self.encoding = kwargs.get("encoding", self.DEFAULT_FILE_ENCODING)
        self.file_extension = kwargs.get("file_extension", self.DEFAULT_FILE_EXTENSION)

        self.upload_folder = os.path.join(upload_folder, user)
        self.user_folder = kwargs.get("user_folder", self.DEFAULT_USER_FOLDER)

        self.format_pkt = packets.DataFormatPacket(
            field_terminator=kwargs.get("field_terminator", self.DEFAULT_FIELD_TERMINATOR),
            line_terminator=kwargs.get("line_terminator", self.DEFAULT_LINE_TERMINATOR),
            field_wrapper=kwargs.get("field_wrapper", self.DEFAULT_FIELD_WRAPPER),
            date_format=kwargs.get("date_format", self.DEFAULT_DATE_FORMAT))

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

    async def _write_frame(self, frame, file_path, format_pkt):
        self.logger.info("Writing frame file: %s", file_path)

        await self.loop.run_in_executor(self.executor, write_frame,
            frame, file_path, format_pkt)

    async def process(self, message):
        import_pkt = message.get_packet(packets.ImportTablePacket)
        format_pkt = message.get_packet(packets.DataFormatPacket,
            default=self.format_pkt, add_if_missing=True)

        for frame_pkt in message.pop_packets(packets.DataFramePacket):
            chunk_filename = self._get_file_path(import_pkt.table, frame_pkt.index)
            message.add_packet(packets.DataFilePacket(chunk_filename))

            await self._write_frame(frame_pkt.frame, chunk_filename, format_pkt)

        await message.forward()
