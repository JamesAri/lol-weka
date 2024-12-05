from datetime import datetime
from typing import Dict, List
import os
import logging
import json
import asyncio
import aiofiles
from aiocsv import AsyncWriter

from services.riot_api import MatchDto
import config

logger = logging.getLogger(__name__)

# TODO: this whole file needs to be refactored...


def __read_match_file(json_match_filepath: str) -> Dict:
    if not json_match_filepath.endswith('.json'):
        logger.warning(f"[!] The file {json_match_filepath} is not a JSON file, skipping")
        return None

    with open(json_match_filepath, 'r') as file:
        try:
            data = json.load(file)
            return data
        except json.JSONDecodeError as json_error:
            logger.error(f"[!] Error decoding JSON from file {json_match_filepath}: {json_error}")
            return None


def __parse_headers_from_snapshot() -> List[str]:
    match_snapshot = config.riot_api['match_snapshot']
    data = __read_match_file(match_snapshot)

    if data is None:
        raise ValueError(f"[!] Corrupted snapshot file {match_snapshot}")

    match_snapshot_dto = MatchDto(data, participant_puuid='any')
    return match_snapshot_dto.get_keys()


def __ensure_directories_exist():
    # CSV export directory
    csv_export_dir = config.exports['csv_export_dir']
    os.makedirs(csv_export_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return os.path.abspath(f'{csv_export_dir}/csv_export_{timestamp}.csv')


HEADERS = __parse_headers_from_snapshot()
EXPORT_FILENAME = __ensure_directories_exist()


class ExportStatisticsWorker:

    data_keys: list[str] = HEADERS  # TODO: FIX THIS

    export_filename: str = EXPORT_FILENAME  # TODO: FIX THIS

    def __transform_match_data(self, match_data) -> List[str] | None:
        match_dto = MatchDto(match_data)
        match_date = datetime.fromtimestamp(match_dto.game_creation/1000).strftime('%Y-%m-%d %H:%M:%S')
        match_hour = datetime.fromtimestamp(match_dto.game_creation/1000).strftime('%H')

        match_data_dict = match_dto.get_as_dict()

        game_mode = match_data_dict.get('game_mode', '')
        is_valid_game_mode = game_mode == 'CLASSIC' or game_mode == 'ARAM'
        if is_valid_game_mode is False:
            return None

        return [match_data_dict.get(key, '') for key in self.data_keys] + [match_date, match_hour]

    async def __read_match_file(self, json_match_filepath: str) -> Dict:
        if not json_match_filepath.endswith('.json'):
            logger.warning(f"[!] The file {json_match_filepath} is not a JSON file, skipping")
            return None

        async with aiofiles.open(json_match_filepath, mode='r') as file:
            try:
                data = json.loads(await file.read())
                return data
            except json.JSONDecodeError as json_error:
                logger.error(f"[!] Error decoding JSON from file {json_match_filepath}: {json_error}")
                return None

    async def run_read(self, filepaths_queue: asyncio.Queue[str], match_data_queue: asyncio.Queue):
        try:
            while not filepaths_queue.empty():
                json_match_filepath = await filepaths_queue.get()
                data = await self.__read_match_file(json_match_filepath)

                if data is None:
                    filepaths_queue.task_done()
                    continue

                match_data = self.__transform_match_data(data)

                # Unwanted match data
                if (match_data is None):
                    filepaths_queue.task_done()
                    continue

                await match_data_queue.put(match_data)
                filepaths_queue.task_done()

            logger.info("[*] Worker finished reading match files")

        except Exception as e:
            logger.exception(f"[!] An error occurred while reading match statistics: {e}")
            raise

    async def run_write(self, match_data_queue: asyncio.Queue):
        try:
            async with aiofiles.open(self.export_filename, 'w') as f:
                writer = AsyncWriter(f)

                await writer.writerow(self.data_keys + ['match_date', 'match_hour'])

                while True:
                    data = await match_data_queue.get()
                    if data is None:
                        logger.info("[*] Export to csv finished!")
                        break

                    await writer.writerow(data)
                    match_data_queue.task_done()

            logger.info(f"[*] Exported matches statistics to {self.export_filename}")

        except Exception as e:
            logger.exception(f"[!] An error occurred while exporting match statistics: {e}")
            raise
