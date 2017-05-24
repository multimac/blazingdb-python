"""
Defines the RepeatedTimer class, for repeatedly calling a given function
"""

from threading import Timer


class RepeatedTimer(object):
    """ Repeatedly calls a given function at consistent intervals """

    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.is_running = False

        self.function = function
        self.interval = interval

        self.args = args
        self.kwargs = kwargs

    def __enter__(self):
        self.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def _run(self):
        self.is_running = False

        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        """ Begins the repeating timer """
        if not self.is_running:
            self.is_running = True

            self._timer = Timer(self.interval, self._run)
            self._timer.start()

    def stop(self):
        """ Ends the repeating timer """
        self._timer.cancel()
        self.is_running = False
