import os
import logging
from datetime import datetime
import json
import pandas as pd

from services.riot_api_dto import MatchDto
import config

logger = logging.getLogger(__name__)


class ExportStatisticsWorker:

    @staticmethod
    def __validate_match_file(json_match_filepath: str):
        if not json_match_filepath.endswith('.json'):
            raise ValueError(f"[!] The file {json_match_filepath} is not a JSON file.")

    @staticmethod
    async def run(json_match_filepath: str):
        try:
            ExportStatisticsWorker.__validate_match_file(json_match_filepath)

            csv_export_dir = config.exports['csv_dir']
            os.makedirs(csv_export_dir, exist_ok=True)

            with open(json_match_filepath, 'r') as file:
                data = json.load(file)

            match_dto = MatchDto(riot_match_dto=data)
            match_id = match_dto.match_id
            match_created_datetime = datetime.fromtimestamp(match_dto.game_creation/1000).strftime('%Y-%m-%d %H:%M:%S')

            df = pd.DataFrame(dict(match_id=match_id, date=match_created_datetime))

            export_filename = f'{csv_export_dir}/{match_id}.csv'
            df.to_csv(export_filename, index=True)
            logger.info(f"[+] Exported match statistics to {os.path.abspath(export_filename)}")
        except Exception as e:
            logger.exception(f"[!] An error occurred while exporting match statistics: {e}")
            raise
