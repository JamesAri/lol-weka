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


class ExportStatisticsWorker:
    _instance = None

    headers: List[str] | None = None
    export_filename: str

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ExportStatisticsWorker, cls).__new__(cls)
            cls._instance.__ensure_export_filename()
        return cls._instance

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

    def __ensure_export_filename(self):
        csv_export_dir = config.exports['csv_export_dir']
        os.makedirs(csv_export_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        self.export_filename = os.path.abspath(f'{csv_export_dir}/csv_export_{timestamp}.csv')

    def __transform_match_data(self, match_data) -> List[str | int] | None:
        # TODO: refactor this mess

        # Get base match dto
        match_dto = MatchDto(match_data)

        # Filter out unwanted PVE game modes
        if match_dto.team_data is None:
            return None

        # Filter out unwanted PVP game modes
        game_mode = match_dto.metadata.get('gameMode', '')
        if game_mode != 'CLASSIC' and game_mode != 'ARAM':
            return None

        # Filter out games which took less than 5 minutes
        if match_dto.metadata.get('gameDuration', 0) < 300:
            return None

        # Flattened object for export
        match_dto_dict = {
            **match_dto.metadata,
            **match_dto.team_data,
        }

        # Compute extended headers
        for participant in match_dto.participants:
            for key, value in participant.items():
                # won't filter bools
                if not isinstance(value, (int, float, complex)):
                    continue

                if participant['teamId'] == match_dto.friendly_team['teamId']:
                    dict_key = f"friendly_team_{key}"
                else:
                    dict_key = f"enemy_team_{key}"

                if match_dto_dict.get(dict_key, None) is None:
                    match_dto_dict[dict_key] = 0

                if isinstance(value, bool):
                    match_dto_dict[dict_key] = bool(value if value else match_dto_dict[dict_key])
                else:  # value is number
                    match_dto_dict[dict_key] += value

        match_dto_dict['matchDate'] = datetime.fromtimestamp(match_dto_dict['gameCreation']/1000).strftime('%Y-%m-%d %H:%M:%S')
        match_dto_dict['matchHour'] = datetime.fromtimestamp(match_dto_dict['gameCreation']/1000).strftime('%H')
        match_dto_dict['win'] = match_dto.friendly_team['win']

        # filter out unwanted columns
        match_dto_dict = {k: v for k, v in match_dto_dict.items() if k in config.CSV_EXPORT_COLUMNS}

        if ExportStatisticsWorker.headers is None:
            ExportStatisticsWorker.headers = list(match_dto_dict.keys())

        return [match_dto_dict.get(key, '') for key in ExportStatisticsWorker.headers]

    async def run_read(self, filepaths_queue: asyncio.Queue[str], match_data_queue: asyncio.Queue):
        try:
            while not filepaths_queue.empty():
                json_match_filepath = await filepaths_queue.get()
                raw_match_data = await self.__read_match_file(json_match_filepath)

                # Unreadable match data
                if raw_match_data is None:
                    filepaths_queue.task_done()
                    continue

                match_data = self.__transform_match_data(raw_match_data)

                # Unwanted match data
                if (match_data is None):
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

                while ExportStatisticsWorker.headers is None:
                    logger.debug("[>] Waiting for headers to be set")
                    await asyncio.sleep(0.5)

                await writer.writerow(ExportStatisticsWorker.headers)

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
