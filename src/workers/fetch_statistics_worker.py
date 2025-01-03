from tqdm import tqdm
import json
import os
import logging
import psycopg
import aiofiles

from db.repository.matches_repository import MatchesRepository
from services.riot_api import RiotApiService
import config

logger = logging.getLogger(__name__)


class FetchStatisticsWorker:
    cur: psycopg.cursor
    matches_repository: MatchesRepository = MatchesRepository()  # TODO: do this via Dependency Injection
    riot_api_service: RiotApiService

    def __init__(self, cur, riot_api_service):
        self.cur = cur
        self.riot_api_service = riot_api_service

    async def run(self, last_match_id=None):
        try:
            match_files_dir = config.exports['match_files_dir']
            os.makedirs(match_files_dir, exist_ok=True)

            all_matches = await self.matches_repository.get_matches_older_than(cur=self.cur, match_id=last_match_id)
            logger.info(f"[+] Began processing {len(all_matches)} matches: {all_matches}")
            all_matches = tqdm(all_matches)

            for match_id in all_matches:
                logger.info(f"[>] Processing match: {match_id}")
                all_matches.set_description("[>] Processing match: %s" % match_id)
                statistics = await self.riot_api_service.get_match_statistics(match_id=match_id)
                # TODO: implement streaming, will require changes in riot api service
                async with aiofiles.open(f"{match_files_dir}/{match_id}.json", 'x', encoding='utf-8') as f:
                    json_string = json.dumps(statistics, ensure_ascii=False, indent=4)
                    await f.write(json_string)
                    logger.info(f"[+] Match {match_id} statistics dumped to file {os.path.abspath(f.name)}")
        except Exception as e:
            logger.exception(f"[!] An error occurred while fetching match statistics: {e}")
            raise
