from tqdm import tqdm
import json
import os
import logging

from db.repository.matches_repository import MatchesRepository
from services.riot_api_service import RiotApiService

logger = logging.getLogger(__name__)


class FetchStatisticsWorker:
    matches_repository: MatchesRepository
    riot_api_service: RiotApiService

    def __init__(self, matches_repository, riot_api_service):
        self.matches_repository = matches_repository
        self.riot_api_service = riot_api_service

    async def run(self, cur, from_match_id=None):
        all_matches = await self.matches_repository.get_matches_older_than(cur=cur, match_id=from_match_id)
        logger.info(f"[+] Began processing {len(all_matches)} matches")

        all_matches = tqdm(all_matches)
        os.makedirs('tmp', exist_ok=True)
        for match_id in all_matches:
            logger.info(f"[>] Processing match: {match_id}")
            all_matches.set_description("[>] Processing match: %s" % match_id)
            statistics = await self.riot_api_service.get_match_statistics(match_id=match_id)
            with open(f"tmp/{match_id}", 'x', encoding='utf-8') as f:
                json.dump(statistics, f, ensure_ascii=False, indent=4)
                logger.info(f"[+] Match {match_id} statistics dumped to file {os.path.abspath(f.name)}")
