"""
Defines a context manager to close a generator upon exiting.
"""


class CountingGenerator(object):

    def __init__(self, generator):
        self.generator = generator

        self.count = 0
        self.iter = None

    def __iter__(self):
        self.iter = iter(self.generator)
        return self

    def __next__(self):
        self.count += 1
        return next(self.iter)


class GeneratorContext(object):  # pylint: disable=too-few-public-methods
    """ Wraps a generator and closes it upon exiting """

    def __init__(self, generator):
        self.generator = generator

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.generator.close()
