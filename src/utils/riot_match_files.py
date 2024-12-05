import json
from typing import Dict, List

from services.riot_api.riot_api_dto import MatchDto


def read_match_file_sync(json_match_filepath: str) -> Dict:
    if not json_match_filepath.endswith('.json'):
        raise ValueError(f"[!] The file {json_match_filepath} is not a JSON file")

    with open(json_match_filepath, 'r') as file:
        try:
            data = json.load(file)
            return data
        except json.JSONDecodeError as json_error:
            raise ValueError(f"[!] Error decoding JSON from file {json_match_filepath}: {json_error}")


def parse_headers_from_snapshot(snapshot_file) -> List[str]:
    match_snapshot = snapshot_file

    try:
        data = read_match_file_sync(match_snapshot)
    except ValueError as e:
        raise ValueError(f"[!] Error parsing headers from snapshot file {snapshot_file}: {e}")

    match_snapshot_dto = MatchDto(data, participant_puuid='any')
    return match_snapshot_dto.get_keys()
