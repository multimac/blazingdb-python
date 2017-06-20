"""
Defines the core message classes used in the pipeline
"""

import abc
import copy
import functools


class Message(object, metaclass=abc.ABCMeta):
    """ Base class used for all messages passed within the pipeline """

    def __init__(self, packets):
        self.msg_id = id(self)
        self.parent = None
        self.stages = None

        self.packets = set(packets)
        self.transport = self._forward

    @classmethod
    def _build_next(cls, msg):
        clone = cls(msg.packets.copy())

        clone.parent = msg
        clone.stages = msg.stages.copy()
        clone.transport = msg.transport

        return clone

    @staticmethod
    async def _forward(msg):
        next_stage = msg.stages.popleft()
        await next_stage.receive(msg)

    def add_packet(self, packet):
        """ Adds the given packet to the message """
        self.packets.add(packet)

    def add_transport(self, transport):
        """ Adds the given transport to the chain of transports for this message """
        self.transport = functools.partial(transport.process, self.transport)

    def get_initial_message(self):
        """ Retrieves the initial message which caused this one to be created """
        current = self
        while current.parent is not None:
            current = current.parent

        return current

    def get_packet(self, packet_type):
        return next(p for p in self.packets if isinstance(p, packet_type))

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

    async def forward(self):
        """ Forwards the message to the next stage in the pipeline """
        msg = Message._build_next(self)
        await self.transport(msg)


class Packet(object):  # pylint: disable=too-few-public-methods
    """ Base class used for all packets delivered with messages """

class ImportTablePacket(Packet):  # pylint: disable=too-few-public-methods
    """ Packet describing a table to be imported """
    def __init__(self, source, src_table, destination, dest_table):
        self.source = source
        self.src_table = src_table
        self.destination = destination
        self.dest_table = dest_table

class DataLoadPacket(Packet):  # pylint: disable=too-few-public-methods
    """ Packet describing a chunk of data to be loaded """
    def __init__(self, data):
        self.data = data

class DataCompletePacket(Packet):  # pylint: disable=too-few-public-methods
    """ Packet notifying later stages the data stream is complete """


class Transport(object, metaclass=abc.ABCMeta):  # pylint: disable=too-few-public-methods
    """ Base class used for all transport classes used to forward messages """

    @abc.abstractmethod
    async def process(self, step, msg):
        """ Forwards the message to the given stage """
