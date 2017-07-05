"""
Defines the series of packets available to messages
"""


# pragma pylint: disable=too-few-public-methods

class Packet(object):
    """ Base class used for all packets delivered with messages """

class DataColumnsPacket(Packet):
    """ Packet describing the columns for load and complete packets """
    def __init__(self, columns):
        self.columns = columns

class DataCompletePacket(Packet):
    """ Packet notifying later stages the data stream is complete """

class DataFilePacket(Packet):
    """ Packet describing a chunk of data in a file to be loaded """
    def __init__(self, file_path, expect_warning):
        self.file_path = file_path
        self.expect_warning = expect_warning

class DataFormatPacket(Packet):
    """ Packet describing the format of a chunk of data """
    def __init__(self, field_terminator, line_terminator, field_wrapper):
        self.field_terminator = field_terminator
        self.line_terminator = line_terminator
        self.field_wrapper = field_wrapper

class DataLoadPacket(Packet):
    """ Packet describing a chunk of data to be loaded """
    def __init__(self, data, index, future=None):
        self.data = data
        self.index = index
        self.future = future

class DestinationPacket(Packet):
    """ Packet describing the destination for the import """
    def __init__(self, destination):
        self.destination = destination

class FuturePacket(Packet):
    """ Packet containing future to be completed later on """
    def __init__(self, future):
        self.future = future

class ImportTablePacket(Packet):
    """ Packet describing a table to be imported """
    def __init__(self, source, table):
        self.source = source
        self.table = table
