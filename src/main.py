import logging
import asyncio
import aiohttp

from db import init_db
import db.repository.matches as matches
import config
from riot_api_service import RiotApiService
from event_loop import run_event_loop
from logger import init_logger

init_logger()
logger = logging.getLogger(__name__)


async def main():
    try:
        logger.info("[*] Bootstrapping the application")

        # Initialize the database connection.
        _, cur, exec, teardown = await init_db()

        # Initialize the aiohttp session.
        session = aiohttp.ClientSession(base_url=config.endpoints['lol_base_url'])

        riot_api_service = RiotApiService(session=session)

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
        if teardown:
            logger.info("[-] Closing database connections")
            await teardown()
        if session:
            logger.info("[-] Closing session connections")
            await session.close()


if __name__ == "__main__":
    run_event_loop(main)
