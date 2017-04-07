""" Pipeline stages for use when importing into BlazingDB """

from blazingdb import exceptions


class BaseStage(object):
    """ Abstract base class for all pipeline stages """

    def begin_import(self, source, importer, connector, table):
        """ Called before data begins being piped through the pipeline """
        pass

    def end_import(self, source, importer, connector, table):
        """ Called after before data begins being piped through the pipeline """
        pass


class CreateTableStage(BaseStage):
    """ Creates the destination table before importing data into BlazingDB """

    def __init__(self, **kwargs):
        self.quiet = kwargs.get("quiet", True)

    def begin_import(self, source, importer, connector, table):
        column_data = [
            "{0} {1}".format(column.name, column.type)
            for column in source.get_columns(table)
        ]
        columns = ", ".join(column_data)

        try:
            connector.query("CREATE TABLE {0} ({1})".format(table, columns))
        except exceptions.QueryException:
            pass


class DropTableStage(BaseStage):
    """ Drops the destination table before importing data into BlazingDB """

    def __init__(self, **kwargs):
        self.only_delete = kwargs.get("only_delete", False)
        self.quiet = kwargs.get("quiet", True)

    def begin_import(self, source, importer, connector, table):
        try:
            connector.query("DELETE FROM {0}".format(table), auto_connect=True)

            if not self.only_delete:
                connector.query("DROP TABLE {0}".format(table), auto_connect=True)

        except exceptions.QueryException:
            pass
