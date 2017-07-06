"""
Defines the core message classes used in the pipeline
"""

import copy
import uuid

from blazingdb import exceptions


class Message(object):
    """ Base class used for all messages passed within the pipeline """

    DEFAULT_MARKER = object()

    def __init__(self, *packets, initial_id=None, handle=None):
        self.msg_id = uuid.uuid4()
        self.initial_id = initial_id if initial_id is not None else self.msg_id

        self.handle = handle
        self.packets = set(iter(packets))
        self.stage_idx = 0
        self.system = None


    def __repr__(self):
        info = []

        info.append("msg_id={!r}".format(self.msg_id))
        info.append("initial_id={!r}".format(self.initial_id))
        info.append("stage_idx={!r}".format(self.stage_idx))

        packet_names = list(pkt.__class__.__name__ for pkt in self.packets)
        info.append("packets={!r}".format(packet_names))

        return "<%s %s>" % (self.__class__.__name__, " ".join(info))

    @classmethod
    def _build_next(cls, msg, packets, track_children):
        if msg.handle is not None:
            clone_handle = msg.handle.create_child(track_children=track_children)
        else:
            clone_handle = None

        clone = cls(*packets, *msg.packets, initial_id=msg.initial_id, handle=clone_handle)
        clone.stage_idx = msg.stage_idx + 1

        return clone

    def complete(self):
        """ Signals the given message as completed """
        if self.handle is None:
            return

        self.handle.complete()

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

    async def forward(self, *packets, system=None, track_children=False):
        """ Forwards the message to the next stage in the pipeline """
        system = system if system is not None else self.system
        msg = Message._build_next(self, packets, track_children)

        await system.enqueue(msg)
        return msg.handle
