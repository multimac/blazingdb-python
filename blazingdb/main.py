"""
Defines a set of helper methods for performing migrations
"""

import asyncio


async def migrate_async(migrator):
    """ Runs the given migrator """
    try:
        await migrator.migrate()
    finally:
        migrator.close()

def shutdown(loop):
    """ Shuts down the given loop, cancelling and completing all tasks """
    shutdown_gens = loop.shutdown_asyncgens()
    loop.run_until_complete(shutdown_gens)

    for task in asyncio.Task.all_tasks(loop):
        task.cancel()

    loop.stop()

    loop.run_forever()
    loop.close()

def migrate(factory):
    """ Performs a migration using the Migrator returned from the given factory function """
    loop = asyncio.new_event_loop()

    try:
        migrator = factory(loop)

        loop.run_until_complete(migrate_async(migrator))
    finally:
        shutdown(loop)

def main():
    raise NotImplementedError("'main' method has not been implemented yet")
