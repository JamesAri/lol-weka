import asyncio
import logging
import psycopg

from db.repository.matches_repository import MatchesRepository
from services.riot_api_service import RiotApiService
from utils.timestamps import get_next_timestamp
from errors import MatchDataNotFoundException

logger = logging.getLogger(__name__)


class FetchMatchesWorker:

    riot_api_service: RiotApiService
    cur: psycopg.cursor
    matches_repository: MatchesRepository = MatchesRepository()  # TODO: do this via Dependency Injection

    def __init__(self, cur, riot_api_service: RiotApiService):
        self.cur = cur
        self.riot_api_service = riot_api_service

    async def __resumed_timestamp(self) -> int | None:
        """ 
        Get the next timestamp (in seconds) based on the last match we have 
        in the database from which we want to resume fetching matches.
        """
        oldest_match_id = await self.matches_repository.get_oldest_match(cur=self.cur)
        if oldest_match_id:
            oldest_match_end_timestamp_ms = await self.riot_api_service.get_match_end_timestamp(match_id=oldest_match_id)
            logger.info(f"[>] Resuming from match: {oldest_match_id}, gameEndTimestamp: {oldest_match_end_timestamp_ms}")
            return get_next_timestamp(oldest_match_end_timestamp_ms)
        return None

    async def run(self, queue: asyncio.Queue, should_resume: bool = False):
        try:
            end_time = None
            if should_resume:
                end_time = await self.__resumed_timestamp()

            while True:
                # Fetch 100 matches at a time
                fetched_matches = await self.riot_api_service.get_matches(start=0, count=100, endTime=end_time)
                logger.info(f"[+] Fetched {len(fetched_matches)} matches: {fetched_matches}")

                # If we didn't fetch any matches, we're done
                if len(fetched_matches) == 0:
                    logger.info("[*] No more matches to fetch")
                    await queue.put(None)
                    return

                await queue.put(fetched_matches)

                # Get the last fetched match (oldest one) and its game end timestamp to refetch older matches from there
                last_match = fetched_matches[-1]
                game_end_timestamp_ms = await self.riot_api_service.get_match_end_timestamp(last_match)
                end_time = get_next_timestamp(game_end_timestamp_ms)
                logger.info(f"[>] Continuing from => match: {last_match}, gameEndTimestamp: {game_end_timestamp_ms} ({end_time})")

        except MatchDataNotFoundException:
            logger.error("[!] Worker failed to fetch match and will terminate now")
            await queue.put(None)
            raise
        except Exception as e:
            logger.exception(f"[!] An error occurred while fetching matches: {e}")
            raise
