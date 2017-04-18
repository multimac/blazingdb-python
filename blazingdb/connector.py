"""
Defines the Connector class to use when connecting to and querying BlazingDB
"""

import asyncio
import logging

import aiohttp

from . import exceptions


class Connector(object): # pylint: disable=too-many-instance-attributes
    """ Handles connecting and querying BlazingDB instances """

    DEFAULT_REQUEST_LIMIT = 5

    def __init__(self, host, user, password, loop=None, **kwargs):
        self.logger = logging.getLogger(__name__)

        conn = aiohttp.TCPConnector(loop=loop, verify_ssl=False)
        self.session = aiohttp.ClientSession(connector=conn, loop=loop)

        self.database = kwargs.get("database")
        self.password = password
        self.token = None
        self.user = user

        protocol = "https" if (kwargs.get("https", True)) else "http"
        port = kwargs.get("port", 8080 if protocol == "http" else 8443)
        request_limit = kwargs.get("request_limit", self.DEFAULT_REQUEST_LIMIT)

        self.baseurl = "{0}://{1}:{2}".format(protocol, host, port)
        self.semaphore = asyncio.BoundedSemaphore(request_limit)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """ Closes the given connector and cleans up the session """
        self.session.close()

        self.password = None
        self.token = None

    def _build_url(self, path):
        """ Builds a url to access the given path in Blazing """
        return "{0}/blazing-jdbc/{1}".format(self.baseurl, path)

    async def _perform_request(self, path, data):
        """ Performs a request against the given path in Blazing """
        url = self._build_url(path)

        self.logger.debug("Performing request to BlazingDB (%s): %s", url, data)

        async with self.semaphore:
            response = await self.session.post(url, data=data, timeout=None)

            self.logger.debug("Retrieved response: %s", response.text())
            return response

    async def _perform_get_results(self, token):
        """ Performs a request to retrieves the results for the given request token """
        data = {"resultSetToken": token, "token": self.token}
        async with await self._perform_request("get-results", data) as response:
            result = await response.json()

        self.logger.warning("Discarding invalidated login token")

        # Currently /get-results invalidates the connection, so this is just to notify
        # the user to reconnect. TODO: Remove when fixed in BlazingDB
        self.token = None

        return result

    async def _perform_query(self, query):
        """ Performs a query against Blazing """
        data = {"username": self.user, "query": query.lower(), "token": self.token}
        async with await self._perform_request("query", data) as response:
            return await response.text()

    async def _perform_register(self):
        """ Performs a register request against Blazing, logging the user in """
        data = {"username": self.user, "password": self.password}
        async with await self._perform_request("register", data) as response:
            return await response.text()

    def is_connected(self):
        """ Determines if the connection is connected to Blazing """
        return self.token is not None

    async def connect(self):
        """ Initialises the connection to Blazing """
        try:
            token = await self._perform_register()
        except aiohttp.ClientError as ex:
            self.logger.exception("Could not log the given user in")
            raise exceptions.RequestException(ex)

        if token == "fail":
            raise exceptions.ConnectionFailedException()

        self.token = token
        self.logger.debug("Retrieved login token %s", self.token)

        if self.database is None:
            return

        try:
            await self._perform_query("USE DATABASE {0}".format(self.database))
        except aiohttp.ClientError as ex:
            self.logger.exception("Failed using specified database for connection")
            raise exceptions.RequestException(ex)

    async def query(self, query, auto_connect=False):
        """ Performs a query against Blazing """
        if not self.is_connected():
            if not auto_connect:
                raise exceptions.NotConnectedException()

            await self.connect()

        try:
            token = await self._perform_query(query)
        except aiohttp.ClientError as ex:
            self.logger.exception("Failed to perform the given query")
            raise exceptions.RequestException(ex)

        if token == "fail":
            raise exceptions.QueryException(query, None)

        try:
            result = await self._perform_get_results(token)
        except aiohttp.ClientError as ex:
            self.logger.exception("Could not retrieve results for the given query")
            raise exceptions.RequestException(ex)

        if result["status"] == "fail":
            raise exceptions.QueryException(query, result)

        return result
