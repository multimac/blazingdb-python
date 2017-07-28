"""
Defines a series of miscellaneous pipeline stages, including:
 - DelayStage
 - PrefixTableStage
 - PromptInputStage
"""

import asyncio
import fnmatch

from . import base, custom
from .. import packets


class DelayStage(custom.CustomActionStage):
    """ Pauses the pipeline before / after importing data """

    def __init__(self, delay, **kwargs):
        super(DelayStage, self).__init__(self._delay, packets.Packet, **kwargs)
        self.prompt = kwargs.get("prompt", "Waiting for input...")
        self.delay = delay

    async def _delay(self, data):  # pylint: disable=unused-argument
        await asyncio.sleep(self.delay)


class InjectPacketStage(custom.CustomActionStage):
    """ Injects a series of packets into messages passing through """

    def __init__(self, *msg_packets, **kwargs):
        super(InjectPacketStage, self).__init__(self._inject, packets.Packet, **kwargs)
        self.msg_packets = msg_packets

    async def _inject(self, message):
        for packet in self.msg_packets:
            message.add_packet(packet)


class PromptInputStage(custom.CustomActionStage):
    """ Prompts for user input to continue before / after importing data """

    def __init__(self, **kwargs):
        super(PromptInputStage, self).__init__(self._prompt, packets.Packet, **kwargs)
        self.prompt = kwargs.get("prompt", "Waiting for input...")

    async def _prompt(self, data):  # pylint: disable=unused-argument
        input(self.prompt)


class SingleFileStage(base.BaseStage):
    """ Collects messages generated from an initial message and processes them one at a time """

    def __init__(self, loop=None):
        super(SingleFileStage, self).__init__(packets.Packet)

        self.loop = loop
        self.semaphores = dict()

    def _get_semaphore(self, msg_id):
        if msg_id not in self.semaphores:
            self.semaphores[msg_id] = asyncio.BoundedSemaphore(loop=self.loop)

        return self.semaphores[msg_id]

    async def process(self, message):
        async with self._get_semaphore(message.initial_id):
            await (await message.forward(track_children=True))


class SkipTableStage(base.BaseStage):
    """ Skips tables based on a given set of inclusions and exclusions """

    def __init__(self, included=None, excluded=None):
        super(SkipTableStage, self).__init__(packets.ImportTablePacket)

        self.included = included
        self.excluded = excluded

    def _filter_table(self, table):
        if self.excluded is not None:
            for pattern in self.excluded:
                if fnmatch.fnmatch(table, pattern):
                    return True

        if self.included is None:
            return False

        for pattern in self.included:
            if fnmatch.fnmatch(table, pattern):
                return False

        return True

    async def process(self, message):
        """ Only calls message.forward if the message isn't filtered """
        import_pkt = message.get_packet(packets.ImportTablePacket)

        if not self._filter_table(import_pkt.table):
            await message.forward()
