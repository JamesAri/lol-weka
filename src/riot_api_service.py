from functools import wraps
import logging

from utils.requests import construct_query_params
import config

logger = logging.getLogger(__name__)


class RiotApiService:
    def __init__(self, session):
        self.session = session

    def rate_limited(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            print("TODO: Implement rate limiting")
            return await func(self, *args, **kwargs)
        return wrapper

    @rate_limited
    async def _GET(self, resource):
        headers = {"X-Riot-Token": config.secrets['api_key']}
        async with self.session.get(url=resource, headers=headers) as response:
            logger.debug(f"[>] GET {response.url}")
            res = await response.json()
            if response.status != 200:
                raise Exception(f"Error: {res}")
            return res

    async def get_matches(self, startTime=None, endTime=None, queue=None, type=None, start=None, count=None):
        arguments = {**locals()}
        del arguments['self']
        query_params = construct_query_params(**arguments)
        resource = f"/lol/match/v5/matches/by-puuid/{config.secrets['puuid']}/ids" + query_params
        return await self._GET(resource=resource)

    async def get_match_statistics(self, match_id):
        resource = f"/lol/match/v5/matches/{match_id}"
        return await self._GET(resource=resource)

    async def get_match_end_timestamp(self, match_id) -> int:
        stats = await self.get_match_statistics(match_id)
        return int(stats['info']['gameEndTimestamp'])
