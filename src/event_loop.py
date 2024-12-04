import signal
import logging
import asyncio


logger = logging.getLogger(__name__)


async def cancel_all_tasks():
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    for task in tasks:
        task.cancel()  # tasks should cleanup in their 'finally' blocks

    logger.info(f"[-] Cancelling {len(tasks)} outstanding tasks")

    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"[!] Task {tasks[idx].get_name()} raised an exception during its execution: {result}")

    except asyncio.CancelledError:
        logger.warning("[!] A task was forcibly cancelled during shutdown")


async def shutdown(signal, loop):
    """
    Running code -> signal -> signal handler -> shutdown -> canceling tasks ->
    -> gathering (waiting for) canceling tasks (cancelling triggers 'finally' blocks in tasks) -> 
    -> printing any unhandled exceptions -> stopping the event loop.
    """
    logger.warning(f"[!] Received exit signal {signal.name}...")

    await cancel_all_tasks()

    stop_event_loop(loop)


def stop_event_loop(loop):
    logger.info("[*] Stopping the event loop")
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
            logger.info("[*] Main task finished")
            # NOTE: This is a choice... main task might have many running tasks and
            # when an error occurs, we might want to cancel all of them if the main
            # task doesn't cancel them itself by the end of its execution.
            # (this is just a safety net, probably not best practice - it's the programmer's
            # responsibility to cancel and await the tasks properly)
            await cancel_all_tasks()
            # Main task finished, stop the loop
            stop_event_loop(loop)
        except asyncio.CancelledError:
            # This could be from signal handlers:
            # Signal received -> calls our signal handlers (shutdown function) ->
            # -> shutdown fn cancels all tasks -> even the main task gets cancelled ->
            # -> we catch the CancelledError here and let the shutdown fn handle the cleanup
            logger.warning("[*] Main task cancelled")
        except:
            # We got unhandled exception/error in the main task
            logger.critical("[!] An unhandled error occurred in the main task", exc_info=True)
            stop_event_loop(loop)
        # Note: we cannot use 'finally' block here, because the main task could
        # be still "cancelling" from signal handlers.

    try:
        loop.create_task(run_main())
        loop.run_forever()
    finally:
        logger.info("[-] Closing the event loop, service is shutting down...")
        loop.close()
