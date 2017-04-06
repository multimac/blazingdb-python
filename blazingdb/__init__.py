"""
Defines the Connector class, used for managing connections to BlazingDB
"""

import logging
import requests

from blazingdb import exceptions


class Connector(object):
    """ Handles connecting and querying BlazingDB instances """

    def __init__(self, host, database, user, password, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.user = user
        self.password = password
        self.database = database
        self.token = None

        protocol = "https" if (kwargs.get("https", True)) else "http"
        port = kwargs.get("port", 8080 if protocol == "http" else 8443)

        self.baseurl = str.format("{0}://{1}:{2}", protocol, host, port)

    def _build_url(self, path):
        """ Builds a url to access the given path in Blazing """
        return str.format("{0}/blazing-jdbc/{1}", self.baseurl, path)

    def _perform_request(self, path, data):
        """ Performs a request against the given path in Blazing """
        return requests.post(self._build_url(path), data, verify=False)

    def _perform_get_results(self, token):
        """ Performs a request to retrieves the results for the given request token """
        data = {"resultSetToken": token, "token": self.token}
        return self._perform_request("get-results", data).json()

    def _perform_query(self, query):
        """ Performs a query against Blazing """
        data = {"query": query, "token": self.token}
        return self._perform_request("query", data).content

    def _perform_register(self):
        """ Performs a register request against Blazing, logging the user in """
        data = {"username": self.user, "password": self.password}
        return self._perform_request("register", data).content

    def is_connected(self):
        """ Determines if the connection is connected to Blazing """
        return self.token is not None

    def connect(self):
        """ Initialises the connection to Blazing """
        try:
            token = self._perform_register()
        except requests.exceptions.RequestException as ex:
            self.logger.exception("Could not log the given user in")
            raise exceptions.RequestException(ex)

        if token == "fail":
            raise exceptions.ConnectionFailedException()

        try:
            self._perform_query(str.format("USE DATABASE {0}", self.database))
        except requests.exceptions.RequestException as ex:
            self.logger.exception("Failed using specified database for connection")
            raise exceptions.RequestException(ex)

        self.token = token

    def query(self, query, auto_connect=False):
        """ Performs a query against Blazing """
        if not self.is_connected():
            if not auto_connect:
                raise exceptions.NotConnectedException()

            self.connect()

        try:
            token = self._perform_query(query)
            return self._perform_get_results(token)
        except requests.exceptions.RequestException as ex:
            self.logger.exception("Failed to perform the given query")
            raise exceptions.RequestException(ex)


class Migrator(object):  # pylint: disable=too-few-public-methods
    """ Handles migrating data from a source into BlazingDB """

    def __init__(self, importer, source):
        self.logger = logging.getLogger(__name__)

        self.importer = importer
        self.source = source

    def migrate(self, tables=None):
        """
        Migrates the given list of tables from the source into BlazingDB. If tables is not
        specified, all tables in the source are migrated
        """

        if tables is None:
            tables = self.source.get_tables()
        elif isinstance(tables, str):
            tables = [tables]

        for table in tables:
            stream = self.source.retrieve(table)
            self.importer.load(table, stream)
