"""
Defines the core message classes used in the pipeline
"""

import copy

from blazingdb import exceptions


# pragma pylint: disable=too-few-public-methods

class Message(object):
    """ Base class used for all messages passed within the pipeline """

    DEFAULT_MARKER = object()

    def __init__(self, *packets):
        self.msg_id = id(self)
        self.parent = None

        self.packets = set(iter(packets))

        self.stage_idx = 0
        self.system = None

    @classmethod
    def _build_next(cls, msg):
        """ Clones a message to be forwarded to the next stage """
        clone = cls(*msg.packets)
        clone.parent = msg

        return clone

    def add_packet(self, packet):
        """ Adds the given packet to the message """
        self.packets.add(packet)

    def get_initial_message(self):
        """ Retrieves the initial message which caused this one to be created """
        current = self
        while current.parent is not None:
            current = current.parent

        return current

    def get_packet(self, packet_type, default=DEFAULT_MARKER):
        """ Retrieves one packet of the given type from the message """
        try:
            check_type = lambda packet: isinstance(packet, packet_type)
            return next(filter(check_type, self.packets))
        except StopIteration:
            if default is not Message.DEFAULT_MARKER:
                return default

            raise exceptions.PacketMissingException(packet_type)

    def get_packets(self, *packet_types):
        """ Retrieves packets of the given types from the message """
        check_type = lambda packet: isinstance(packet, packet_types)
        return set(filter(check_type, self.packets))

    def update_packet(self, packet, **updates):
        """ Updates values in the given packet """
        self.packets.remove(packet)
        packet = copy.copy(packet)

        for key, value in updates.items():
            setattr(packet, key, value)

        self.packets.add(packet)
        return packet

    def remove_packet(self, packet):
        """ Removes the given packet from the message """
        self.packets.remove(packet)

    async def forward(self, *packets):
        """ Forwards the message to the next stage in the pipeline """
        msg = Message._build_next(self)
        msg.packets.update(packets)

        await self.system.process(msg)


class Packet(object):
    """ Base class used for all packets delivered with messages """

class DataColumnsPacket(Packet):
    """ Packet describing the columns for load and complete packets """
    def __init__(self, columns):
        self.columns = columns

class DataCompletePacket(Packet):
    """ Packet notifying later stages the data stream is complete """

class DataFormatPacket(Packet):
    """ Packet describing the format of a chunk of data """
    def __init__(self, field_terminator, line_terminator, field_wrapper):
        self.field_terminator = field_terminator
        self.line_terminator = line_terminator
        self.field_wrapper = field_wrapper

class DataLoadPacket(Packet):
    """ Packet describing a chunk of data to be loaded """
    def __init__(self, data, index):
        self.data = data
        self.index = index

class ImportTablePacket(Packet):
    """ Packet describing a table to be imported """
    def __init__(self, destination, source, table):
        self.destination = destination
        self.source = source
        self.table = table
