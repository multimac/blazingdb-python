""" Defines the exceptions thrown by the BlazingDB module """


class BlazingException(Exception):
    """ Base class for all BlazingDB exceptions """


class ConnectionFailedException(BlazingException):
    """ Thown when something goes wrong attempting to connect to BlazingDB """
    pass


class NotConnectedException(BlazingException):
    """ Thrown when attempting to query an unconnected Connector """
    pass


class RequestException(BlazingException):
    """ Thrown when there is an exception caught from the requests module"""

    def __init__(self, exception):
        super(RequestException, self).__init__()
        self.inner_exception = exception
