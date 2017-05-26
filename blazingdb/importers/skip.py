"""
Defines the SkipImporter class which is a basic implementation of BaseImport that doesn't
import any data.
"""

import logging

from . import base


class SkipImporter(base.BaseImporter):  # pylint: disable=too-few-public-methods
    """ Skips importing any data into Blazing """

    def __init__(self, **kwargs):
        super(SkipImporter, self).__init__()
        self.logger = logging.getLogger(__name__)

    async def load(self, data):
        """ Logs out which table would have been imported int Blazing """
        self.logger.info("Skipping import of table %s", data["dest_table"])
