"""
Defines the Postgres migrator for moving data into BlazingDB from Postgres
"""

import asyncio
import logging

import aiohttp

from blazingdb import exceptions


class BlazingConnector(object):
    """ Handles connecting and querying BlazingDB instances """

    DEFAULT_REQUEST_LIMIT = 5

    SERVER_RESTART_ERROR = "The BlazingDB server is restarting please try again in a moment."

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
        self.semaphore = asyncio.BoundedSemaphore(request_limit, loop=loop)

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
        token = await self._perform_register()
        if token == "fail":
            raise exceptions.ConnectionFailedException()

        self.logger.debug("Retrieved login token %s", token)

        if self.database is not None:
            await self._perform_query("USE DATABASE {0}".format(self.database), token)

        return token

    async def _query(self, query):
        login_token = await self._connect()
        result_token = await self._perform_query(query, login_token)

        if result_token == "fail":
            raise exceptions.QueryException(query, None)

        results = await self._perform_get_results(login_token, result_token)
        if results["status"] == "fail":
            rows = results["rows"]

            if len(rows) == 1 and len(rows[0]) == 1:
                error = rows[0][0]

                if error == self.SERVER_RESTART_ERROR:
                    raise exceptions.ServerRestartException(query, results)

            raise exceptions.QueryException(query, results)

        return results

    async def query(self, query):
        """ Performs a query against Blazing """
        try:
            return await self._query(query)
        except aiohttp.ClientError as ex:
            raise exceptions.QueryException(query, None) from ex
