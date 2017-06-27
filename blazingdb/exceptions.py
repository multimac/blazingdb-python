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


class StoppedException(BlazingException):
    """ Thrown when attempting to call migrate on a stopped migrator """


class PacketMissingException(BlazingException):
    """ Thrown when an attempt is made to retrieve an invalid packet from a message """

    def __init__(self, packet_type):
        super(PacketMissingException, self).__init__()
        self.packet_type = packet_type

    def __str__(self):
        return "packet_type={0}".format(self.packet_type)


class PipelineException(BlazingException):
    """ Thrown when an exception occurs while processing pipeline stages """


class RetryException(BlazingException):
    """ Thrown when an attempt to forward a message fails too many times """


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
