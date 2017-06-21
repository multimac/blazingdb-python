"""
Defines a set of helper methods for performing migrations
"""

import asyncio
import concurrent
import logging
import signal

from .util import sig


def shutdown(loop):
    """ Shuts down the given loop, cancelling and completing all tasks """
    logging.getLogger(__name__).info("Shutting down event loop")

    pending = asyncio.Task.all_tasks(loop)
    gathered = asyncio.gather(*pending, loop=loop, return_exceptions=True)

    try:
        loop.run_until_complete(gathered)
    except:  # pylint: disable=bare-except
        logging.getLogger(__name__).warning("Skipping cancellation of pending tasks")

    shutdown_gens = loop.shutdown_asyncgens()

    try:
        loop.run_until_complete(shutdown_gens)
    except:  # pylint: disable=bare-except
        logging.getLogger(__name__).warning("Skipping shutdown of async generators")

    loop.close()

async def migrate_async(migrator_factory):
    """ Performs a migration using the Migrator returned from the given factory function """
    loop = asyncio.get_event_loop()
    migrator = migrator_factory(loop)

    try:
        await migrator.migrate()
    finally:
        await migrator.shutdown()
        migrator.close()

def migrate(migrator_factory):
    """ Performs a migration using the Migrator returned from the given factory function """
    loop = asyncio.new_event_loop()
    migrator = migrator_factory(loop)

    migration_task = asyncio.ensure_future(migrator.migrate(), loop=loop)

    def _interrupt(sig_num, stack):  # pylint: disable=unused-argument
        nonlocal migration_task

        logging.getLogger(__name__).info("Cancelling import...")
        migration_task.cancel()

    # pragma pylint: disable=multiple-statements

    with sig.SignalContext(signal.SIGINT, _interrupt):
        try: loop.run_until_complete(migration_task)
        except concurrent.futures.CancelledError: pass

    # pragma pylint: enable=multiple-statements

    loop.run_until_complete(migrator.shutdown())

    migrator.close()
    shutdown(loop)

def main():
    raise NotImplementedError("'main' method has not been implemented yet")
