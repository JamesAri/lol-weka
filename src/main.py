import logging
import asyncio
import aiohttp

import config
import toggles
from logger import init_logger
from db import init_db
from event_loop import run_event_loop
from utils.timestamps import get_next_timestamp
from utils.fs_helpers import get_filepaths_from_dir
from workers import StoreMatchesWorker, FetchStatisticsWorker, FetchMatchesWorker, ExportStatisticsWorker
from services.riot_api import RiotApiService

init_logger()
logger = logging.getLogger(__name__)

cur = None
riot_api_service = None
exec = None


async def run_tasks(tasks: list[asyncio.Task], return_exceptions=True):
    results = await asyncio.gather(*tasks, return_exceptions=return_exceptions)
    if return_exceptions:
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                err_msg = f"[!] An error occurred for task {tasks[idx].get_name()}: {result}"
                logger.error(err_msg)
                print(err_msg)


# ===>===>===> FEATURES <===<===<===

async def fetch_matches():
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


async def fetch_statistics():
    # Just one worker for now, we are rate limited anyways
    statistics_fetching_task = asyncio.create_task(
        FetchStatisticsWorker(cur, riot_api_service).run(last_match_id=None),
        name="FetchStatisticsWorker",
    )
    await run_tasks([statistics_fetching_task])


async def export():
    match_files_queue = asyncio.Queue()
    match_data_queue = asyncio.Queue(1)

    filenames = get_filepaths_from_dir(config.exports['match_files_dir'])
    for filename in filenames:
        match_files_queue.put_nowait(filename)
    logger.info(f"[*] Generating csv export from {match_files_queue.qsize()} match files")

    read_tasks = []
    # start 5 tasks to read statistics from JSON files
    for _ in range(5):
        read_tasks.append(
            asyncio.create_task(
                ExportStatisticsWorker().run_read(match_files_queue, match_data_queue),
                name="ExportStatisticsWorker-Read",
            )
        )

    # start 1 task to write the statistics to a CSV file
    write_task = asyncio.create_task(
        ExportStatisticsWorker().run_write(match_data_queue),
        name="ExportStatisticsWorker-Write",
    )

    await run_tasks(read_tasks, return_exceptions=False)

    # Signal the end of the queue
    await match_data_queue.put(None)

    await write_task


async def main():
    global cur, riot_api_service, exec
    try:
        logger.info("[*] Bootstrapping the application")

        # Initialize the database connection.
        _conn, cur, exec, teardown = await init_db()

        # Initialize the aiohttp session and the Riot API service
        session = aiohttp.ClientSession(base_url=config.endpoints['lol_base_url'])
        riot_api_service = RiotApiService(session=session)  # TODO: do this via dependency injection

        if toggles.FETCH_MATCHES_TOGGLE:
            await fetch_matches()
        if toggles.FETCH_STATISTICS_TOGGLE:
            await fetch_statistics()
        if toggles.EXPORT_STATISTICS_TOGGLE:
            await export()

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
