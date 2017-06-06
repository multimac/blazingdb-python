"""
Defines a context manager to close a generator upon exiting.
"""

class GeneratorContext(object):  # pylint: disable=too-few-public-methods
    """ Wraps a generator and closes it upon exiting """

    def __init__(self, generator):
        self.generator = generator

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.generator.close()
