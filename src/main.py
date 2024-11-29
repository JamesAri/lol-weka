import logging
import asyncio
import aiohttp

from init import init
import db.repository.matches as matches
import config
import utils

init()

logger = logging.getLogger(__name__)

# Create a single global session for all requests
# to reuse the connection.
session = None


async def GET_request(resource):
    headers = {"X-Riot-Token": config.secrets['api_key']}
    async with session.get(url=resource, headers=headers) as response:
        return await response.json()


async def get_matches(startTime=None, endTime=None, queue=None, type=None, start=None, count=None):
    arguments = {**locals()}
    query_params = utils.construct_query_params(**arguments)
    resource = f"/lol/match/v5/matches/by-puuid/{config.secrets['puuid']}/ids" + query_params
    return await GET_request(resource=resource)


async def get_match_statistics(match_id):
    resource = f"/lol/match/v5/matches/{match_id}"
    return await GET_request(resource=resource)


async def worker(name, queue):
    while True:
        # Get a "work item" out of the queue.
        sleep_for = await queue.get()

        # Sleep for the "sleep_for" seconds.
        await asyncio.sleep(sleep_for)

        # Notify the queue that the "work item" has been processed.
        queue.task_done()
        logging.debug(f'[^] worker {name} has slept for {sleep_for:.2f} seconds')


async def main():
    logger.info("[*] Bootstrapping the application.")
    global session
    session = aiohttp.ClientSession(base_url=config.endpoints['lol_base_url'])

    try:
        logger.info("[+] Fetching initiated")
        fetched_matches = await get_matches(start=0, count=2)
        logger.info(fetched_matches)

        logger.info(f"[>] Storing matches: {fetched_matches}")
        matches.saveMatches(list(map((lambda match: (match,)), fetched_matches)))
        logger.info("[^] Matches successfully stored")
        
        allMatches = matches.getAllMatches()
        logger.debug(f"Checking db integrity: {allMatches}")
        
        for match in fetched_matches:
            stats = await get_match_statistics(match)
            logger.info("[.] Got match statistics")
            # todo: process the statistics and store them in db
            
            logger.debug("[.] Sleeping for 1 second")
            await asyncio.sleep(1)


    except Exception as e:
        logger.exception(f"An error occurred:\n{e}")
        print(f"An error occurred:\n{e}")
    finally:
        await session.close()

asyncio.run(main())
