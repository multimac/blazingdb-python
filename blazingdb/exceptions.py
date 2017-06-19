"""
Defines all the exceptions which can be thrown by the BlazingDB package
"""

import textwrap


class BlazingException(Exception):
    """ Base class for all BlazingDB exceptions """


class ConnectionFailedException(BlazingException):
    """ Thown when something goes wrong attempting to connect to BlazingDB """


class MigrateException(BlazingException):
    """ Thrown when an exception occurs when migrating a table """


class PipelineException(BlazingException):
    """ Thrown when an exception occurs while processing pipeline stages """


class QueryException(BlazingException):
    """ Thrown when something goes wrong attempting to query BlazingDB """

    def __init__(self, query, response):
        super(QueryException, self).__init__()
        self.query = query
        self.response = response

    def __str__(self):
        return "query='{0}', response='{1}'".format(self.query, self.response)


class RequestException(BlazingException):
    """ Thrown when there is an exception caught from the requests module"""

    def __init__(self, status, response):
        super(RequestException, self).__init__()
        self.status = status
        self.response = response

    def __str__(self):
        response_message = textwrap.shorten(self.response, 100)
        return "status='{0}', response='{1}'".format(self.status, response_message)


class ServerRestartException(BlazingException):
    """ Thrown when the BlazingDB is restarting """

    def __init__(self, query, response):
        super(ServerRestartException, self).__init__()
        self.query = query
        self.response = response

    def __str__(self):
        return "query='{0}', response='{1}'".format(self.query, self.response)


class SkipImportException(BlazingException):
    """ Thrown when an import should be skipped """
