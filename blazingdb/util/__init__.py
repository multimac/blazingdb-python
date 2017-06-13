"""
Package containing utility modules
"""


async def aenumerate(iterable, start=0):
    """ Asynchronous implementation of enumerate builtin """
    i = start

    async for item in iterable:
        yield (i, item)
        i += 1

__all__ = ["gen", "timer"]
