import signal
import logging
import asyncio


logger = logging.getLogger(__name__)


async def shutdown(signal, loop):
    logger.warning(f"Received exit signal {signal.name}...")

    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    for task in tasks:
        task.cancel()  # tasks should cleanup in their 'finally' blocks

    logger.debug(f"Cancelling {len(tasks)} outstanding tasks")

    try:
        # TODO: check task(s) results (and exceptions/errors)
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        logger.warning("A task was forcibly cancelled during shutdown")

    logger.debug("Stopping the event loop")
    loop.stop()


def run_event_loop(main):
    loop = asyncio.new_event_loop()

    for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

    async def run_main():
        """
        Runs the main coroutine and ensures the loop stops afterwards.
        """
        try:
            # Run the main task
            await main()
            # Main task finished, stop the loop
            loop.stop()
        except asyncio.CancelledError as e:
            # This could be from signal handlers
            logger.warning("[*] Main task cancelled")
        except:
            # We got unhandled exception/error in the main task
            logger.critical("An unhandled error occurred in the main task")
            loop.stop()
        # Note: we cannot use 'finally' block here, because the main task could
        # be still "cancelling" from signal handlers.

    try:
        loop.create_task(run_main())
        loop.run_forever()
    finally:
        logger.info("[-] Closing the event loop, service is shutting down...")
        loop.close()
