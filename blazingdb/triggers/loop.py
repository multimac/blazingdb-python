"""
Defines a test trigger which queues a single table continuously
"""

from . import base


# pylint: disable=too-few-public-methods

class LoopTrigger(base.TableTrigger):
    """ A simple trigger which continuously returns the same table """

    def __init__(self, source, table):
        super(LoopTrigger, self).__init__(source)
        self.table = table

    async def _poll(self):
        while True: yield self.table  # pylint: disable=multiple-statements
