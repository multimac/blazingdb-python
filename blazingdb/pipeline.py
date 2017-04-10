""" Pipeline stages for use when importing into BlazingDB """

import logging

from blazingdb import exceptions


class BaseStage(object):
    """ Abstract base class for all pipeline stages """

    def begin_import(self, source, importer, connector, table):
        """ Called before data begins being piped through the pipeline """
        pass

    def end_import(self, source, importer, connector, table):
        """ Called after before data begins being piped through the pipeline """
        pass


class PrefixTableStage(BaseStage):
    """ Prefixes the destination tables """

    def __init__(self, prefix):
        self.prefix = prefix

    def begin_import(self, source, importer, connector, data):
        data["dest_table"] = "{0}_{1}".format(self.prefix, data["dest_table"])


class CreateTableStage(BaseStage):
    """ Creates the destination table before importing data into BlazingDB """

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.quiet = kwargs.get("quiet", True)

    def begin_import(self, source, importer, connector, data):
        table = data["dest_table"]

        column_data = [
            "{0} {1}".format(column["name"], column["type"])
            for column in source.get_columns(data["src_table"])
        ]
        columns = ", ".join(column_data)

        self.logger.info("Creating table %s with %s column(s)", table, len(column_data))
        self.logger.debug("Columns: %s", columns)

        try:
            connector.query("CREATE TABLE {0} ({1})".format(table, columns), auto_connect=True)
        except exceptions.QueryException:
            message = " ".join([
                "QueryException caught when attempting to create table, assuming "
                "this means the table has already been created"
            ])

            self.logger.debug(message)


class DropTableStage(BaseStage):
    """ Drops the destination table before importing data into BlazingDB """

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.only_delete = kwargs.get("only_delete", False)
        self.quiet = kwargs.get("quiet", True)

    def begin_import(self, source, importer, connector, data):
        table = data["dest_table"]

        self.logger.info("Dropping table %s (only_delete %s)", table, self.only_delete)

        try:
            connector.query("DELETE FROM {0}".format(table), auto_connect=True)

            if not self.only_delete:
                connector.query("DROP TABLE {0}".format(table), auto_connect=True)
        except exceptions.QueryException:
            message = " ".join([
                "QueryException caught when attempting to create table, assuming "
                "this means the table has already been dropped"
            ])

            self.logger.debug(message)
