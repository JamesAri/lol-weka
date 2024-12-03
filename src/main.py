import logging
import asyncio
import aiohttp

import config
import toggles
from logger import init_logger
from db import init_db
from event_loop import run_event_loop
from utils.timestamps import get_next_timestamp
from workers import StoreMatchesWorker, FetchStatisticsWorker, FetchMatchesWorker, ExportStatisticsWorker
from services.riot_api_service import RiotApiService

init_logger()
logger = logging.getLogger(__name__)


async def run_tasks(tasks: list[asyncio.Task]):
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            err_msg = f"[!] An error occurred for task {tasks[idx].get_name()}: {result}"
            logger.error(err_msg)
            print(err_msg)


async def main():
    try:
        logger.info("[*] Bootstrapping the application")

        # Initialize the database connection.
        _, cur, exec, teardown = await init_db()

        # Initialize the aiohttp session and the Riot API service
        session = aiohttp.ClientSession(base_url=config.endpoints['lol_base_url'])
        riot_api_service = RiotApiService(session=session)  # TODO: do this via dependency injection

        if toggles.FETCH_MATCHES_TOGGLE:
            matches_queue = asyncio.Queue(5)
            match_fetching_task = asyncio.create_task(
                FetchMatchesWorker(cur, riot_api_service).run(queue=matches_queue, should_resume=toggles.SHOULD_RESUME_TOGGLE),
                name="FetchMatchesWorker",
            )
            match_storing_task = asyncio.create_task(
                StoreMatchesWorker(exec).run(queue=matches_queue),
                name="StoreMatchesWorker",
            )
            await run_tasks([match_fetching_task, match_storing_task])

        if toggles.FETCH_STATISTICS_TOGGLE:
            statistics_fetching_task = asyncio.create_task(
                FetchStatisticsWorker(cur, riot_api_service).run(from_match_id=None),
                name="FetchStatisticsWorker",
            )
            await run_tasks([statistics_fetching_task])

        if toggles.EXPORT_STATISTICS_TOGGLE:
            # TODO: enable for a whole directory, not just one match file
            statistics_exporting_task = asyncio.create_task(
                ExportStatisticsWorker.run('tmp/EUN1_3346334676.json'),
                name="ExportStatisticsWorker",
            )
            await run_tasks([statistics_exporting_task])

    except Exception as e:
        logger.critical(f"[!] An error occurred: {e}", exc_info=True)
        print(f"[!] An error occurred: {e}")
    finally:
        if teardown:
            logger.info("[-] Closing database connection")
            await teardown()
        if session:
            logger.info("[-] Closing session connection")
            await session.close()


if __name__ == "__main__":
    run_event_loop(main)
