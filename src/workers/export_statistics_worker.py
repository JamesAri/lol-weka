from datetime import datetime
from typing import List
import os
import logging
import json
import asyncio
import aiofiles
from aiocsv import AsyncWriter

from services.riot_api_dto import MatchDto
import config

logger = logging.getLogger(__name__)


class ExportStatisticsWorker:

    @classmethod
    def __transform_match_data(cls, match_dto: MatchDto) -> List:
        return [
            match_dto.match_id,
            datetime.fromtimestamp(match_dto.game_creation/1000).strftime('%Y-%m-%d %H:%M:%S')
        ]

    @classmethod
    async def run_read(cls, filepaths_queue: asyncio.Queue[str], match_data_queue: asyncio.Queue):
        try:
            while not filepaths_queue.empty():
                json_match_filepath = await filepaths_queue.get()

                if not json_match_filepath.endswith('.json'):
                    logger.warning(f"[!] The file {json_match_filepath} is not a JSON file, skipping")
                    filepaths_queue.task_done()
                    continue

                async with aiofiles.open(json_match_filepath, mode='r') as file:
                    try:
                        data = json.loads(await file.read())
                    except json.JSONDecodeError as json_error:
                        logger.error(f"[!] Error decoding JSON from file {json_match_filepath}: {json_error}")
                        filepaths_queue.task_done()
                        continue

                match_dto = MatchDto(riot_match_dto=data)
                match_data = cls.__transform_match_data(match_dto)
                await match_data_queue.put(match_data)

                filepaths_queue.task_done()

            await match_data_queue.put(None)
            logger.info("[*] Worker finished reading match files")

        except Exception as e:
            logger.exception(f"[!] An error occurred while exporting match statistics: {e}")
            raise

    @classmethod
    async def run_write(cls, number_of_producers: int, match_data_queue: asyncio.Queue):
        try:
            none_marks_read = 0

            csv_export_dir = config.exports['csv_export_dir']
            os.makedirs(csv_export_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            export_filename = f'{csv_export_dir}/csv_export_{timestamp}.csv'

            async with aiofiles.open(export_filename, 'w') as f:
                writer = AsyncWriter(f)
                # TODO: ugly
                await writer.writerow(['match_id', 'date'])

                while True:
                    data = await match_data_queue.get()
                    if data is None:
                        none_marks_read += 1
                        if none_marks_read < number_of_producers:
                            match_data_queue.task_done()
                            continue
                        logger.info("[*] Export to csv finished!")
                        break

                    await writer.writerow(data)
                    match_data_queue.task_done()

            logger.info(f"[*] Exported matches statistics to {os.path.abspath(export_filename)}")

        except Exception as e:
            logger.exception(f"[!] An error occurred while exporting match statistics: {e}")
            raise
