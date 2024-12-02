import logging
import asyncio
import aiohttp

import config
from logger import init_logger
from db import init_db
from db.repository import MatchesRepository
from riot_api_service import RiotApiService
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


async def fetch_matches(exec, end_time=None):
    while True:
        # Fetch 100 matches at a time
        fetched_matches = await riot_api_service.get_matches(start=0, count=100, endTime=end_time)
        logger.info(f"[+] Fetched matches: {fetched_matches}")

        # If we didn't fetch any matches, we're done
        if len(fetched_matches) == 0:
            logger.info("[*] No more matches to fetch")
            return

        # Store the matches in the database (TODO: do this in a separate worker)
        logger.info(f"[^] Storing matches")
        await matches_repository.save_matches(exec=exec, matches=list(map((lambda match: (match,)), fetched_matches)))

        # Get the last fetched match (oldest one) and its game end timestamp to refetch older matches from there
        last_match = fetched_matches[-1]
        game_end_timestamp_ms = await riot_api_service.get_match_end_timestamp(last_match)
        end_time = get_next_timestamp(game_end_timestamp_ms)
        logger.info(f"[.] Continuing from => match: {last_match}, gameEndTimestamp: {game_end_timestamp_ms} ({end_time})")


async def fetch_statistics(cur):
    # TODO:
    # - create table for statistics
    # - make workers to process the statistics and save them to db (asyncio.Queue)
    pass


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

        # TODO: work_queue = asyncio.Queue()

        # Initialize the database connection.
        _, cur, exec, teardown = await init_db()

        # Initialize the aiohttp session and the Riot API service
        session = aiohttp.ClientSession(base_url=config.endpoints['lol_base_url'])
        riot_api_service = RiotApiService(session=session)

        #### FETCHING MATCHES ####
        # If the service restarts/crashes, we want to resume from the oldest match we have in the database.
        # TODO: we could still want to fetch the games we played (will play) later
        end_time = await resumed_timestamp(cur=cur)
        await fetch_matches(exec=exec, end_time=end_time)

        #### FETCHING MATCHES STATISTICS ####
        await fetch_statistics(cur=cur)

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
