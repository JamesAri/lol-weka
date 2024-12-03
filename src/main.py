from tqdm import tqdm
import json
import os
import logging
import asyncio
import aiohttp

import config
import toggles
from logger import init_logger
from db import init_db
from db.repository import MatchesRepository
from services.riot_api_service import RiotApiService
from event_loop import run_event_loop
from utils.throttled_task_runner import ThrottledTaskRunner, RateLimit

init_logger()
logger = logging.getLogger(__name__)

# TODO: do this via dependency injection
matches_repository = MatchesRepository()
riot_api_service = None


def get_next_timestamp(timestamp_ms: int) -> int:
    """
    We iterate the matches from the newest to the oldest.
    This function returns the timestamp of the next match to fetch.

    Timestamps returned from the Riot API are in milliseconds, but the
    MatchV5 API accepts seconds, so we need to convert them.

    @param timestamp_ms: timestamp in milliseconds (as retrieved from the API)
    @return: timestamp in seconds of the next match to fetch
    """
    # -1 s to avoid fetching the last match again
    return int(timestamp_ms / 1000) - 1


async def fetch_worker(queue: asyncio.Queue, end_time=None):
    while True:
        # Fetch 100 matches at a time
        fetched_matches = await riot_api_service.get_matches(start=0, count=100, endTime=end_time)
        logger.info(f"[+] Fetched {len(fetched_matches)} matches: {fetched_matches}")

        # If we didn't fetch any matches, we're done
        if len(fetched_matches) == 0:
            logger.info("[*] No more matches to fetch")
            await queue.put(None)
            return

        await queue.put(fetched_matches)

        # Get the last fetched match (oldest one) and its game end timestamp to refetch older matches from there
        last_match = fetched_matches[-1]
        game_end_timestamp_ms = await riot_api_service.get_match_end_timestamp(last_match)
        end_time = get_next_timestamp(game_end_timestamp_ms)
        logger.info(f"[>] Continuing from => match: {last_match}, gameEndTimestamp: {game_end_timestamp_ms} ({end_time})")


async def store_worker(exec, queue: asyncio.Queue):
    while True:
        matches = await queue.get()
        if matches is None:
            logger.info("[*] No more matches to store")
            return
        logger.info(f"[+] Storing {len(matches)} matches: {matches}")
        await matches_repository.save_matches(exec=exec, matches=list(map((lambda match: (match,)), matches)))
        queue.task_done()


async def fetch_statistics(cur, from_match_id=None):
    all_matches = await matches_repository.get_matches_older_than(cur=cur, match_id=from_match_id)
    logger.info(f"[+] Began processing {len(all_matches)} matches")

    all_matches = tqdm(all_matches)
    os.makedirs('tmp', exist_ok=True)
    for match_id in all_matches:
        logger.info(f"[>] Processing match: {match_id}")
        all_matches.set_description("[>] Processing match: %s" % match_id)
        statistics = await riot_api_service.get_match_statistics(match_id=match_id)
        with open(f"tmp/{match_id}", 'x', encoding='utf-8') as f:
            json.dump(statistics, f, ensure_ascii=False, indent=4)
            logger.info(f"[+] Match {match_id} statistics dumped to file {os.path.abspath(f.name)}")


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
            # If the service restarts/crashes, we want to resume from the oldest match we have in the database.
            # TODO: we could still want to fetch the games we played (will play) later
            end_time = await resumed_timestamp(cur=cur) if toggles.SHOULD_RESUME_TOGGLE else None
            fetching_task = asyncio.create_task(fetch_worker(end_time=end_time, queue=matches_queue))
            storing_task = asyncio.create_task(store_worker(exec=exec, queue=matches_queue))
            await asyncio.gather(fetching_task, storing_task)

        if toggles.FETCH_STATISTICS_TOGGLE:
            await fetch_statistics(cur=cur, from_match_id=None)

    except Exception as e:
        logger.exception(f"An error occurred:\n{e}")
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
