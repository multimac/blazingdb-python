"""
Defines the exceptions thrown by the BlazingDB module
"""


class BlazingException(Exception):
    """ Base class for all BlazingDB exceptions """


class ConnectionFailedException(BlazingException):
    """ Thown when something goes wrong attempting to connect to BlazingDB """
    pass


class NotConnectedException(BlazingException):
    """ Thrown when attempting to query an unconnected Connector """
    pass


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

    def __init__(self, exception):
        super(RequestException, self).__init__()
        self.inner_exception = exception

    def __str__(self):
        return "inner_exception='{0}'".format(self.inner_exception)
