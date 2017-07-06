import asyncio
import functools

class Handle(object):
    def __init__(self, loop=None, track_following=False):
        loop = loop if loop is not None else asyncio.get_event_loop()

        self.followers = [] if track_following else None
        self.future = loop.create_future()
        self.loop = loop

    def __await__(self):
        yield from self.future.__await__()
        
        if self.followers is not None:
            yield from asyncio.wait(self.followers, loop=self.loop)

    def add_follower(self, follower):
        if self.followers is None:
            return

        self.followers.append(follower)

    def complete(self):
        self.future.set_result(None)
