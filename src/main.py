import logging
import asyncio
import aiohttp
import signal

from logger import init_logger
from db import init_db
import db.repository.matches as matches
import config
from riot_api_service import RiotApiService

init_logger()
logger = logging.getLogger(__name__)


async def main():
    try:
        logger.info("[*] Bootstrapping the application.")

        # Initialize the database connection.
        _, cur, exec, teardown = await init_db()

        # Initialize the aiohttp session.
        session = aiohttp.ClientSession(base_url=config.endpoints['lol_base_url'])

        riot_api_service = RiotApiService(session=session)

        await asyncio.sleep(1000)

        # TODO: store all matches
        # - set request rate limits
        # - create logic for continuous fetching of matches
        logger.info("[+] Fetching initiated")
        fetched_matches = await riot_api_service.get_matches(start=0, count=2)
        logger.info(fetched_matches)

        logger.info(f"[>] Storing matches: {fetched_matches}")
        await matches.saveMatches(exec=exec, matches=list(map((lambda match: (match,)), fetched_matches)))

        allMatches = await matches.getAllMatches(cur=cur)
        logger.debug(f"Checking db integrity: {allMatches}")

        # TODO: process the statistics and store them in db
        # - set request rate limits
        # - create table for statistics
        # - make workers to process the statistics and save them to db (asyncio.Queue)
        # - store the statistics in db
        for match in fetched_matches:
            logger.info("[>] Fetching match statistics")
            stats = await riot_api_service.get_match_statistics(match)
            logger.debug("[.] Sleeping for 1 second")
            await asyncio.sleep(1)
    except Exception as e:
        logger.exception(f"An error occurred:\n{e}")
        print(f"An error occurred:\n{e}")
    finally:
        logger.info("[-] Closing database connections")
        await teardown()
        logger.info("[-] Closing session connections")
        await session.close()


asyncio.run(main())


# async def shutdown(signal, loop):
#     """Cleanup tasks tied to the service's shutdown."""

#     logger.info(f"Received exit signal {signal.name}...")

#     tasks = [t for t in asyncio.all_tasks() if t is not
#              asyncio.current_task()]

#     [task.cancel() for task in tasks]

#     logger.info(f"Cancelling {len(tasks)} outstanding tasks")
#     await asyncio.gather(*tasks, return_exceptions=True)

#     loop.stop()


# def run():
#     loop = asyncio.get_event_loop()

#     loop.add_signal_handler(signal.SIGHUP, lambda: asyncio.create_task(shutdown(signal.SIGHUP, loop)))
#     loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(shutdown(signal.SIGTERM, loop)))
#     loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(shutdown(signal.SIGINT, loop)))

#     try:
#         loop.create_task(main())
#         loop.run_forever()
#     finally:
#         loop.close()
#         logger.info("[-] Successfully shutdown the service loop.")


# if __name__ == "__main__":
#     run()
