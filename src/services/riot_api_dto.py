from typing import Any, Dict, List

import config

# https://developer.riotgames.com/apis#match-v5/GET_getMatch


class MatchDto:

    participant: Dict

    def __init__(self, riot_match_dto: Dict):
        self.__parse_riot_match_dto(riot_match_dto)

    def __filter_local_participant(self, info_dto: Dict):
        puuid = config.riot_api['puuid']

        for participant in info_dto['participants']:
            if participant['puuid'] == puuid:
                self.participant = participant
                break
        if self.participant is None:
            raise ValueError(f"[!] The participant with puuid {puuid} was not found in the match data")

    def get_keys(self) -> List[str]:
        return [
            'match_id',
            'game_mode',
            'game_creation',
            'game_start_timestamp',
            'game_end_timestamp',
            'game_duration',
            *self.participant.keys()
        ]

    def get_as_dict(self) -> Dict[str, Any]:
        return {
            'match_id': self.match_id,
            'game_mode': self.game_mode,
            'game_creation': self.game_creation,
            'game_start_timestamp': self.game_start_timestamp,
            'game_end_timestamp': self.game_end_timestamp,
            'game_duration': self.game_duration,
            **self.participant
        }

    def __parse_riot_match_dto(self, riot_match_dto: Dict):
        try:
            # Base data objects
            info_dto = riot_match_dto['info']
            metadata_dto = riot_match_dto['metadata']

            # Common for each match
            self.match_id = metadata_dto['matchId']
            self.game_mode = info_dto['gameMode']
            self.game_creation = info_dto['gameCreation']
            self.game_start_timestamp = info_dto['gameStartTimestamp']
            self.game_end_timestamp = info_dto['gameEndTimestamp']
            self.game_duration = info_dto['gameDuration']

            # Participant data may differ based on game version
            self.__filter_local_participant(info_dto)
            self.participant.pop('challenges', None)
            self.participant.pop('perks', None)

        except KeyError as e:
            raise KeyError(f"[!] The Riot Match DTO ({self.match_id}) is missing a required key: {e}")
