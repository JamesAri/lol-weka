import asyncio
import logging

import utils
import config

logger = logging.getLogger(__name__)


class RiotApiService:
    def __init__(self, session):
        self.session = session

    async def GET_request(self, resource):
        headers = {"X-Riot-Token": config.secrets['api_key']}
        async with self.session.get(url=resource, headers=headers) as response:
            return await response.json()

    async def get_matches(self, startTime=None, endTime=None, queue=None, type=None, start=None, count=None):
        arguments = {**locals()}
        del arguments['self']
        query_params = utils.construct_query_params(**arguments)
        resource = f"/lol/match/v5/matches/by-puuid/{config.secrets['puuid']}/ids" + query_params
        return await self.GET_request(resource=resource)

    async def get_match_statistics(self, match_id):
        resource = f"/lol/match/v5/matches/{match_id}"
        return await self.GET_request(resource=resource)

    async def worker(name, queue):
        while True:
            # Get a "work item" out of the queue.
            sleep_for = await queue.get()

            # Sleep for the "sleep_for" seconds.
            await asyncio.sleep(sleep_for)

            # Notify the queue that the "work item" has been processed.
            queue.task_done()
            logging.debug(f'[^] worker {name} has slept for {sleep_for:.2f} seconds')
