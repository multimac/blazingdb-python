"""
Defines the base stage class for use during data migration
"""

class BaseStage(object):
    """ Base class for all pipeline stages """

    def begin_import(self, source, importer, connector, table):
        """ Called before data begins being piped through the pipeline """
        pass

    def end_import(self, source, importer, connector, table):
        """ Called after before data begins being piped through the pipeline """
        pass
