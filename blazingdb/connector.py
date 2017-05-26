"""
Defines the Connector class to use when connecting to and querying BlazingDB
"""

import asyncio
import logging

import aiohttp

from . import exceptions


class Connector(object):
    """ Handles connecting and querying BlazingDB instances """

    DEFAULT_REQUEST_LIMIT = 5

    def __init__(self, host, user, password, loop=None, **kwargs):
        self.logger = logging.getLogger(__name__)

        conn = aiohttp.TCPConnector(loop=loop, verify_ssl=False)
        self.session = aiohttp.ClientSession(connector=conn, loop=loop)

        self.database = kwargs.get("database")
        self.password = password
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
        self.password = None
        self.session.close()

    def _build_url(self, path):
        """ Builds a url to access the given path in Blazing """
        return "{0}/blazing-jdbc/{1}".format(self.baseurl, path)

    async def _perform_request(self, path, data, callback):
        """ Performs a request against the given path in Blazing """
        url = self._build_url(path)

        self.logger.debug("Performing request to BlazingDB (%s): %s", url, data)

        async with self.semaphore:
            async with self.session.post(url, data=data, timeout=None) as response:
                if response.status != 200:
                    raise exceptions.RequestException(response.status, await response.text())

                result = await callback(response)

                response.close()
                return result

    async def _perform_get_results(self, login_token, result_token):
        """ Performs a request to retrieves the results for the given request token """
        data = {"resultSetToken": result_token, "token": login_token}
        return await self._perform_request("get-results", data, lambda r: r.json())

    async def _perform_query(self, query, login_token):
        """ Performs a query against Blazing """
        data = {"username": self.user, "query": query.lower(), "token": login_token}
        return await self._perform_request("query", data, lambda r: r.text())

    async def _perform_register(self):
        """ Performs a register request against Blazing, logging the user in """
        data = {"username": self.user, "password": self.password}
        return await self._perform_request("register", data, lambda r: r.text())

    async def _connect(self):
        """ Initialises the connection to Blazing """
        try:
            token = await self._perform_register()
        except:
            self.logger.exception("Could not log the given user in")
            raise

        if token == "fail":
            raise exceptions.ConnectionFailedException()

        self.logger.debug("Retrieved login token %s", token)

        if self.database is not None:
            try:
                await self._perform_query("USE DATABASE {0}".format(self.database), token)
            except:
                self.logger.exception("Failed using specified database for connection")
                raise

        return token

    async def query(self, query):
        """ Performs a query against Blazing """
        login_token = await self._connect()

        try:
            result_token = await self._perform_query(query, login_token)
        except:
            self.logger.exception("Failed to perform the given query")
            raise

        if result_token == "fail":
            raise exceptions.QueryException(query, None)

        try:
            result = await self._perform_get_results(login_token, result_token)
        except:
            self.logger.exception("Could not retrieve results for the given query")
            raise

        if result["status"] == "fail":
            raise exceptions.QueryException(query, result)

        return result
