"""
Defines the core message classes used in the pipeline
"""

import copy


class Message(object):
    """ Base class used for all messages passed in the pipeline """

    def __init__(self):
        self.msg_id = id(self)

        self.parent = None
        self.stages = None
        self.msgs = []

    def _rebuild(self, parent, stages):
        self.msg_id = id(self)

        self.parent = parent
        self.stages = stages.copy()

    def get_messages(self, types):
        """ Retrieves all contained messages of the given types """
        check_type = lambda msg: isinstance(msg, types)
        return set(filter(check_type, self.msgs))

    def get_parent(self, types):
        """ Retrieves the closest parent of one of the given types """
        current = self.parent
        while current is not None:
            if isinstance(current, types):
                return current

            current = current.parent

        return None

    async def forward(self, msg=None, **updates):
        """ Forwards the given message on to the next stage in the pipeline """
        if msg is None:
            msg = copy.copy(self)
        elif updates:
            msg = copy.copy(msg)

        for key, value in updates.items():
            setattr(self, key, value)

        # pragma pylint: disable=protected-access
        msg._rebuild(self, self.stages)
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


class LoadCompleteMessage(Message):
    """ Message indicating there is no more data to be loaded """


class LoadDataMessage(Message):
    """ Message defining a batch of data to be imported """
    def __init__(self, table, source, data):
        super(LoadDataMessage, self).__init__()
        self.table = table
        self.source = source
        self.data = data
