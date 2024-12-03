import logging
import asyncio
import aiohttp

import config
import toggles
from logger import init_logger
from db import init_db
from event_loop import run_event_loop
from utils.timestamps import get_next_timestamp
from workers import StoreMatchesWorker, FetchStatisticsWorker, FetchMatchesWorker

# ===>===> TODO: do this via dependency injection ===>===>
from db.repository import MatchesRepository
from services.riot_api_service import RiotApiService

matches_repository = MatchesRepository()
riot_api_service = None
# <===<===<===<===<===<===<===<===<===<===<===<===<===<===

init_logger()
logger = logging.getLogger(__name__)


async def resumed_timestamp(cur) -> int | None:
    """ 
    Get the next timestamp (in seconds) based on the last match we have 
    in the database from which we want to resume fetching matches.
    """
    oldest_match_id = await matches_repository.get_oldest_match(cur=cur)
    if oldest_match_id:
        oldest_match_end_timestamp_ms = await riot_api_service.get_match_end_timestamp(match_id=oldest_match_id)
        logger.info(f"[>] Resuming from match: {oldest_match_id}, gameEndTimestamp: {oldest_match_end_timestamp_ms}")
        return get_next_timestamp(oldest_match_end_timestamp_ms)
    return None


async def run_tasks(tasks: list[asyncio.Task]):
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            err_msg = f"[!] An error occurred for task {tasks[idx].get_name()}: {result}"
            logger.error(err_msg)
            print(err_msg)


async def main():
    global riot_api_service
    try:
        logger.info("[*] Bootstrapping the application")

        # Initialize the database connection.
        _, cur, exec, teardown = await init_db()

        # Initialize the aiohttp session and the Riot API service
        session = aiohttp.ClientSession(base_url=config.endpoints['lol_base_url'])
        riot_api_service = RiotApiService(session=session)

        if toggles.FETCH_MATCHES_TOGGLE:
            matches_queue = asyncio.Queue(5)
            end_time = await resumed_timestamp(cur=cur) if toggles.SHOULD_RESUME_TOGGLE else None
            match_fetching_task = asyncio.create_task(
                FetchMatchesWorker(riot_api_service).run(end_time=end_time, queue=matches_queue),
                name="FetchMatchesWorker",
            )
            match_storing_task = asyncio.create_task(
                StoreMatchesWorker(matches_repository).run(exec=exec, queue=matches_queue),
                name="StoreMatchesWorker",
            )
            await run_tasks([match_fetching_task, match_storing_task])

        if toggles.FETCH_STATISTICS_TOGGLE:
            statistics_fetching_task = asyncio.create_task(
                FetchStatisticsWorker(matches_repository, riot_api_service).run(cur=cur, from_match_id=None),
                name="FetchStatisticsWorker",
            )
            await run_tasks([statistics_fetching_task])

    except Exception as e:
        logger.exception(f"[!] An error occurred:\n{e}")
        print(f"An error occurred:\n{e}")
    finally:
        if teardown:
            logger.info("[-] Closing database connection")
            await teardown()
        if session:
            logger.info("[-] Closing session connection")
            await session.close()


if __name__ == "__main__":
    run_event_loop(main)
