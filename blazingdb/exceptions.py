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

    def __init__(self, query):
        super(QueryException, self).__init__()
        self.query = query


class RequestException(BlazingException):
    """ Thrown when there is an exception caught from the requests module"""

    def __init__(self, exception):
        super(RequestException, self).__init__()
        self.inner_exception = exception
