"""
Defines the SkipImporter class which is a basic implementation of BaseImport that doesn't
import any data.
"""

import logging

from . import base


class SkipImporter(base.BaseImporter):  # pylint: disable=too-few-public-methods
    """ Skips importing any data into Blazing """

    class SkipBatcher(object):  # pylint: disable=too-few-public-methods
        """ Fake batcher to skip importing any data """

        @staticmethod
        def batch(stream):  # pylint: disable=unused-argument
            """ Generates a series of batches from the stream """
            raise GeneratorExit

    def __init__(self, **kwargs):
        super(SkipImporter, self).__init__(self.SkipBatcher())
        self.logger = logging.getLogger(__name__)

    def _init_load(self, data):
        self.logger.info("Skipping import of table %s", data["dest_table"])

    async def _load_batch(self, data, batch):
        pass
