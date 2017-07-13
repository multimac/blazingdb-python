"""
Defines several helper methods for messages
"""

from . import packets


async def get_columns(message, add_if_missing=False):
    """ Retrieves a DataColumnsPacket, or creates one from an ImportTablePacket """
    columns_pkt = message.get_packet(packets.DataColumnsPacket, default=None)

    if columns_pkt is not None:
        return columns_pkt.columns

    import_pkt = message.get_packet(packets.ImportTablePacket)

    table = import_pkt.table
    source = import_pkt.source
    columns = await source.get_columns(table)

    if add_if_missing:
        message.add_packet(packets.DataColumnsPacket(columns))

    return columns
