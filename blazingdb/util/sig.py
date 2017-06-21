"""
Defines the SignalContext class
"""

import signal


# pragma pylint: disable=too-few-public-methods

class SignalContext(object):
    """ Manages setting and resetting signal handlers """
    def __init__(self, sig_num, handler):
        self.sig_num = sig_num

        self.handler = handler
        self.previous = None

    def __enter__(self):
        self.previous = signal.signal(self.sig_num, self.handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.signal(self.sig_num, self.previous)
