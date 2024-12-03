from functools import wraps
import logging
import aiohttp

from utils.requests import construct_query_params
from utils.throttled_task_runner import ThrottledTaskRunner, RateLimit
import config
from errors import MatchDataNotFoundException

logger = logging.getLogger(__name__)


class RiotApiService:

    __ttr: ThrottledTaskRunner

    session: aiohttp.ClientSession

    def __init__(self, session):
        self.session = session
        self.headers = {"X-Riot-Token": config.secrets['api_key']}
        rate_limits = [
            RateLimit(value=config.rate_limits['per_second'], time_window=1),
            RateLimit(value=config.rate_limits['per_minute'], time_window=60),
        ]
        self.__ttr = ThrottledTaskRunner(rate_limits=rate_limits)

    def rate_limited(func):
        @wraps(func)
        async def wrapper(self: 'RiotApiService', *args, **kwargs):
            return await self.__ttr.run(self, *args, cb=func, **kwargs)
        return wrapper

    @rate_limited
    async def _GET(self, resource):
        async with self.session.get(url=resource, headers=self.headers) as response:
            logger.info(f"[>] GET {response.url}")
            res = await response.json()
            if response.status == 404:
                # Data not found, happens for older matches that are no longer available.
                logger.warning(f"[^] 404 - Data not found: {res} for {response.url}")
                raise MatchDataNotFoundException
            elif response.status != 200:
                raise Exception(f"Error: {res}")
            return res

    async def get_matches(self, startTime=None, endTime=None, queue=None, type=None, start=None, count=None):
        arguments = {**locals()}
        del arguments['self']
        query_params = construct_query_params(**arguments)
        resource = f"/lol/match/v5/matches/by-puuid/{config.riot_api['puuid']}/ids" + query_params
        return await self._GET(resource=resource)

    async def get_match_statistics(self, match_id):
        resource = f"/lol/match/v5/matches/{match_id}"
        return await self._GET(resource=resource)

    async def get_match_end_timestamp(self, match_id) -> int:
        try:
            stats = await self.get_match_statistics(match_id)
            return int(stats['info']['gameEndTimestamp'])
        except TypeError:
            raise RuntimeError(f"Got malformed response for match {match_id}")
