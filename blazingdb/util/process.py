"""
Defines a custom ProcessPoolExecutor which allows for initializing processes the
concurrent.futures.ProcessPoolExecutor by executing a given function
"""

import concurrent
from concurrent.futures.process import _process_worker

import multiprocessing
import signal


def quiet_sigint():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def _init_process_worker(call_queue, result_queue, initializer, init_args):
    if initializer is not None:
        initializer(*init_args)

    return _process_worker(call_queue, result_queue)

class ProcessPoolExecutor(concurrent.futures.ProcessPoolExecutor):
    """ Extended ProcessPoolExecutor allowing for initialization of processes """

    def __init__(self, initializer, *init_args, max_workers=None):
        super(ProcessPoolExecutor, self).__init__(max_workers=max_workers)

        self._initializer = initializer
        self._init_args = init_args

    def _adjust_process_count(self):
        for _ in range(len(self._processes), self._max_workers):
            args = (self._call_queue, self._result_queue, self._initializer, self._init_args)

            process = multiprocessing.Process(target=_init_process_worker, args=args)
            process.start()

            self._processes[process.pid] = process
