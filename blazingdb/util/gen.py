"""
Defines a context manager to close a generator upon exiting.
"""


# pragma pylint: disable=too-few-public-methods

class CountingGenerator(object):
    """ Counts the number of items yielded from a generator """

    def __init__(self, generator):
        self.generator = generator

        self.count = 0
        self.iter = None

    async def __aiter__(self):
        self.iter = self.generator.__aiter__()
        return self

    async def __anext__(self):
        item = await self.iter.__anext__()

        self.count += 1
        return item

    def __iter__(self):
        self.iter = iter(self.generator)
        return self

    def __next__(self):
        item = next(self.iter)

        self.count += 1
        return item


class GeneratorContext(object):
    """ Wraps a generator and closes it upon exiting """

    def __init__(self, generator):
        self.generator = generator

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.generator.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.generator.close()
