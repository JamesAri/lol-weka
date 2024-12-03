import asyncio
import logging

from services.riot_api_service import RiotApiService
from utils.timestamps import get_next_timestamp
from errors import MatchDataNotFoundException

logger = logging.getLogger(__name__)


class FetchMatchesWorker:

    riot_api_service: RiotApiService

    def __init__(self, riot_api_service):
        self.riot_api_service = riot_api_service

    async def run(self, queue: asyncio.Queue, end_time=None):
        try:
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
