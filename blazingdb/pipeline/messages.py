"""
Defines the core message classes used in the pipeline
"""

import itertools
import operator


class Message(tuple):
    """ Base class used for all messages passed in the pipeline """
    __slots__ = ()

    msg_id = property(operator.itemgetter(0))
    stages = property(operator.itemgetter(1))

    msgs = property(operator.itemgetter(2))

    def __new__(cls, msg_id=0):
        return tuple.__new__(cls, (msg_id, None, []))

    def __init__(self):
        super(Message, self).__init__()
        self.msgs.append(self)

    def get_messages(self, types):
        """ Retrieves all messages of the given types """
        arg_list = itertools.product(self.msgs, types)
        check_type = lambda args: isinstance(*args)

        return set(filter(check_type, arg_list))

    async def forward(self, msg=None):
        """ Forwards the given message on to the next stage in the pipeline """
        msg = msg if msg is not None else self

        msg.stages = self.stages.copy()
        next_stage = msg.stages.popleft()

        await next_stage.receive(msg)


class ImportTableMessage(Message):
    """ Message indicating that the given table should be imported """
    def __init__(self, source, src_table, dest_table, destination):
        super(ImportTableMessage, self).__init__()
        self.source = source
        self.src_table = src_table
        self.dest_table = dest_table
        self.destination = destination

class LoadDataMessage(Message):
    def __init__(self, destination, table, data):
        super()
