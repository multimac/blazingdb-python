"""
Package containing all the different importers used to load data into BlazingDB
"""

from collections import namedtuple

from .chunking import ChunkingImporter
from .skip import SkipImporter
from .stream import StreamImporter

RowFormat = namedtuple("RowFormat", ["field_terminator", "line_terminator", "field_wrapper"])

__all__ = ["base", "chunking", "skip", "stream"]
