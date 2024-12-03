import asyncio
import logging

from db.repository.matches_repository import MatchesRepository

logger = logging.getLogger(__name__)


class StoreMatchesWorker:

    matches_repository: MatchesRepository = MatchesRepository()  # TODO: do this via Dependency Injection

    async def run(self, exec, queue: asyncio.Queue):
        try:
            while True:
                matches = await queue.get()
                if matches is None:
                    logger.info("[*] No more matches to store")
                    return
                logger.info(f"[+] Storing {len(matches)} matches: {matches}")
                await self.matches_repository.save_matches(exec=exec, matches=list(map((lambda match: (match,)), matches)))
                queue.task_done()
        except Exception as e:
            logger.exception(f"[!] An error occurred while storing matches: {e}")
            raise
