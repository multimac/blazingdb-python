"""
Defines the Migrator class which can be used for migrating data into BlazingDB
"""

import logging


class Migrator(object):  # pylint: disable=too-few-public-methods
    """ Handles migrating data from a source into BlazingDB """

    def __init__(self, connector, source, pipeline, importer):
        self.logger = logging.getLogger(__name__)

        self.connector = connector
        self.importer = importer
        self.pipeline = pipeline
        self.source = source

    def _migrate_table(self, table):
        """ Imports an individual table into BlazingDB """
        import_data = {
            "dest_table": table,
            "src_table": table,
            "stream": self.source.retrieve(table)
        }

        for stage in self.pipeline:
            stage.begin_import(self.source, self.importer, self.connector, import_data)

        self.importer.load(self.connector, import_data)

        for stage in self.pipeline:
            stage.end_import(self.source, self.importer, self.connector, import_data)

    def migrate(self, tables=None):
        """
        Migrates the given list of tables from the source into BlazingDB. If tables is not
        specified, all tables in the source are migrated
        """

        if tables is None:
            tables = self.source.get_tables()
        elif isinstance(tables, str):
            tables = [tables]

        self.logger.info("Tables to be imported: %s", ", ".join(tables))

        for i, table in enumerate(tables):
            self.logger.info("Importing table %s of %s, %s", i, len(tables), table)
            self._migrate_table(table)
