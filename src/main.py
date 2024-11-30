import logging
import asyncio
import aiohttp

from db import init_db
from db.repository import MatchesRepository
import config
from riot_api_service import RiotApiService
from event_loop import run_event_loop
from logger import init_logger

init_logger()
logger = logging.getLogger(__name__)


async def fetch_matches(session, exec):
    # TODO: store all matches
    # - set request rate limits
    # - create logic for continuous fetching of matches
    matches_repository = MatchesRepository()
    riot_api_service = RiotApiService(session=session)

    logger.info("[>] Fetching initiated")
    fetched_matches = await riot_api_service.get_matches(start=0, count=2)
    logger.info(f"[+] Fetched matches: {fetched_matches}")

    logger.info(f"[>] Storing matches: {fetched_matches}")
    await matches_repository.saveMatches(exec=exec, matches=list(map((lambda match: (match,)), fetched_matches)))


async def fetch_statistics(session, cur):
    matches_repository = MatchesRepository()
    riot_api_service = RiotApiService(session=session)

    allMatches = await matches_repository.getAllMatches(cur=cur)
    # TODO: process the statistics and store them in db
    # - set request rate limits
    # - create table for statistics
    # - make workers to process the statistics and save them to db (asyncio.Queue)
    # - store the statistics in db
    logger.info(f"[>] Fetching match statistics for {len(allMatches)} entries")
    for match in allMatches:
        logger.info(f"[+] Processing match: {match}")
        stats = await riot_api_service.get_match_statistics(match)
        logger.debug("[.] Sleeping for 1 second")
        await asyncio.sleep(1)


async def main():
    try:
        logger.info("[*] Bootstrapping the application")

        # Initialize the database connection.
        _, cur, exec, teardown = await init_db()

        # Initialize the aiohttp session.
        session = aiohttp.ClientSession(base_url=config.endpoints['lol_base_url'])

        await fetch_matches(session=session, exec=exec)
        await fetch_statistics(session=session, cur=cur)

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
