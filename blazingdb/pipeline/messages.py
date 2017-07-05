"""
Defines the core message classes used in the pipeline
"""

import copy
import uuid

from blazingdb import exceptions


class Message(object):
    """ Base class used for all messages passed within the pipeline """

    DEFAULT_MARKER = object()

    def __init__(self, *packets):
        self.msg_id = uuid.uuid4()
        self.packets = set(iter(packets))

        self.stage_idx = 0
        self.system = None

    def __repr__(self):
        info = []

        info.append("msg_id={!r}".format(self.msg_id))
        info.append("stage_idx={!r}".format(self.stage_idx))

        packet_names = list(pkt.__class__.__name__ for pkt in self.packets)
        info.append("packets={!r}".format(packet_names))

        return "<%s %s>" % (self.__class__.__name__, " ".join(info))

    def _build_next(self, packets):
        clone = copy.copy(self)
        clone.packets = self.packets.union(packets)
        clone.stage_idx += 1

        return clone

    def add_packet(self, packet):
        """ Adds the given packet to the message """
        self.packets.add(packet)

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

    async def forward(self, *packets, wait=False, system=None):
        """ Forwards the message to the next stage in the pipeline """
        system = system if system is not None else self.system
        msg = self._build_next(packets)

        if wait:
            await system.process(msg)
        else:
            await system.enqueue(msg)
