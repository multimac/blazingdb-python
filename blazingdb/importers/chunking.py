"""
Defines the ChunkingImporter class which handles splitting data up into individual chunks,
writing them to disk and then importing them into BlazingDB
"""

import logging
from os import path

import aiofiles

from . import base
from ..util import gen


class ChunkingImporter(base.BaseImporter):  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """ Handles the loading of data into Blazing using flat files """

    DEFAULT_BUFFER_SIZE = -1
    DEFAULT_FILE_ENCODING = "utf-8"
    DEFAULT_FILE_EXTENSION = "dat"
    DEFAULT_USER_FOLDER = "data"

    def __init__(self, upload_folder, user, loop=None, **kwargs):
        super(ChunkingImporter, self).__init__(loop, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.loop = loop

        self.upload_folder = path.join(upload_folder, user)
        self.user_folder = kwargs.get("user_folder", self.DEFAULT_USER_FOLDER)

        self.buffer_size = kwargs.get("buffer_size", self.DEFAULT_BUFFER_SIZE)
        self.encoding = kwargs.get("encoding", self.DEFAULT_FILE_ENCODING)
        self.file_extension = kwargs.get("file_extension", self.DEFAULT_FILE_EXTENSION)
        self.ignore_skipdata = kwargs.get("ignore_skipdata", False)

    def _open_file(self, filename):
        return aiofiles.open(
            filename, "w", buffering=self.buffer_size,
            encoding=self.encoding, loop=self.loop
        )

    def _get_filename(self, table, chunk):
        filename = "{0}_{1}".format(table, chunk)
        if self.file_extension is None:
            return filename

        return "{0}.{1}".format(filename, self.file_extension)

    def _get_import_path(self, table, chunk):
        """ Generates a path for a given chunk of a table to be used in a query """
        filename = self._get_filename(table, chunk)
        if self.user_folder is None:
            return filename

        return path.join(self.user_folder, filename)

    def _get_file_path(self, table, chunk):
        """ Generates a path for a given chunk of a table to be used for writing chunks """
        import_path = self._get_import_path(table, chunk)
        return path.join(self.upload_folder, import_path)

    async def _write_chunk(self, data):
        """ Writes a chunk of data to disk """
        table = data["dest_table"]
        index = data["index"]

        chunk_filename = self._get_file_path(table, index)

        self.logger.info("Writing chunk file: %s", chunk_filename)

        stream = gen.CountingGenerator(data["stream"])
        async with self._open_file(chunk_filename) as chunk_file:
            async for row in stream:
                await chunk_file.write(row)

        return stream.count

    async def _load_chunk(self, data):
        """ Loads a chunk of data into Blazing """
        connector = data["connector"]
        table = data["dest_table"]
        index = data["index"]
        fmt = data["format"]

        query_filename = self._get_import_path(table, index)

        style = "infile" if not self.ignore_skipdata else "infilenoskip"
        method = "{0} {1}".format(style, query_filename)

        self.logger.info("Loading chunk %s into blazing", query_filename)
        await self._perform_request(connector, method, fmt, table)

    async def load(self, data):
        if await self._write_chunk(data) == 0:
            self.logger.info("Skipping %s as no rows were retrieved", data["src_table"])
            return

        await self._load_chunk(data)
