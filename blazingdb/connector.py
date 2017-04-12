"""
Defines the Connector class to use when connecting to and querying BlazingDB
"""

import logging
import requests

from . import exceptions


class Connector(object):
    """ Handles connecting and querying BlazingDB instances """

    def __init__(self, host, user, password, **kwargs):
        self.logger = logging.getLogger(__name__)

        self.user = user
        self.password = password

        self.database = kwargs.get("database")
        self.token = None

        protocol = "https" if (kwargs.get("https", True)) else "http"
        port = kwargs.get("port", 8080 if protocol == "http" else 8443)

        self.baseurl = "{0}://{1}:{2}".format(protocol, host, port)

    def _build_url(self, path):
        """ Builds a url to access the given path in Blazing """
        return "{0}/blazing-jdbc/{1}".format(self.baseurl, path)

    def _perform_request(self, path, data):
        """ Performs a request against the given path in Blazing """
        url = self._build_url(path)

        self.logger.debug("Performing request to BlazingDB (%s): %s", url, data)

        return requests.post(url, data, verify=False)

    def _perform_get_results(self, token):
        """ Performs a request to retrieves the results for the given request token """
        data = {"resultSetToken": token, "token": self.token}
        result = self._perform_request("get-results", data).json()

        self.logger.warning("Discarding invalidated login token")

        # Currently /get-results invalidates the connection, so this is just to notify
        # the user to reconnect. TODO: Remove when fixed in BlazingDB
        self.token = None

        return result

    def _perform_query(self, query):
        """ Performs a query against Blazing """
        data = {"username": self.user, "query": query.lower(), "token": self.token}
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

        self.token = token
        self.logger.debug("Retrieved login token %s", self.token)

        if self.database is None:
            return

        try:
            self._perform_query("USE DATABASE {0}".format(self.database))
        except requests.exceptions.RequestException as ex:
            self.logger.exception("Failed using specified database for connection")
            raise exceptions.RequestException(ex)

    def query(self, query, auto_connect=False):
        """ Performs a query against Blazing """
        if not self.is_connected():
            if not auto_connect:
                raise exceptions.NotConnectedException()

            self.connect()

        try:
            token = self._perform_query(query)
        except requests.exceptions.RequestException as ex:
            self.logger.exception("Failed to perform the given query")
            raise exceptions.RequestException(ex)

        if token == "fail":
            raise exceptions.QueryException(query, None)

        try:
            result = self._perform_get_results(token)
        except requests.exceptions.RequestException as ex:
            self.logger.exception("Could not retrieve results for the given query")
            raise exceptions.RequestException(ex)
        except ValueError as ex:
            self.logger.exception("Could not parse response as JSON")
            raise exceptions.QueryException(query, None)

        if result["status"] == "fail":
            raise exceptions.QueryException(query, result)

        return result