"""
Defines a set of helper methods for performing migrations
"""

import asyncio
import concurrent
import contextlib
import logging
import multiprocessing
import signal


async def migrate_async(migrator_factory):
    loop = asyncio.get_event_loop()
    async with migrator_factory(loop) as migrator:
        try:
            await migrator.migrate()
        finally:
            await migrator.shutdown()

def shutdown_loop(loop, executor):
    """ Shuts down the given loop, cancelling and completing all tasks """
    logging.getLogger(__name__).info("Shutting down event loop")

    pending = asyncio.Task.all_tasks(loop)
    gathered = asyncio.gather(*pending, loop=loop, return_exceptions=True)

    try:
        loop.run_until_complete(gathered)
    except KeyboardInterrupt:
        logging.getLogger(__name__).warning("Ignoring pending tasks")
        gathered.cancel()

    try:
        shutdown_gens = loop.shutdown_asyncgens()
        loop.run_until_complete(shutdown_gens)
    except KeyboardInterrupt:
        logging.getLogger(__name__).warning("Skipping shutdown of async generators")

    executor.shutdown(wait=True)
    loop.close()

def migrate(migrator_factory):
    """ Performs a migration using the Migrator returned from the given factory function """
    if multiprocessing.get_start_method(allow_none=True) is None:
        multiprocessing.set_start_method("forkserver")

    executor = concurrent.futures.ThreadPoolExecutor()

    loop = asyncio.new_event_loop()
    loop.set_default_executor(executor)

    migration_task = asyncio.ensure_future(migrate_async(migrator_factory), loop=loop)

    def _interrupt():  # pylint: disable=unused-argument
        nonlocal migration_task

        logging.getLogger(__name__).info("Cancelling import...")
        migration_task.cancel()

    loop.add_signal_handler(signal.SIGINT, _interrupt)

    with contextlib.suppress(concurrent.futures.CancelledError):
        loop.run_until_complete(migration_task)

    loop.remove_signal_handler(signal.SIGINT)

    shutdown_loop(loop, executor)

def main():
    raise NotImplementedError("'main' method has not been implemented yet")
