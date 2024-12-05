from typing import Any, Dict, List

import config
from errors import ParticipantNotFoundException

# https://developer.riotgames.com/apis#match-v5/GET_getMatch


class MatchDto:

    participant_puuid: str
    participant: Dict = None

    def __init__(self, riot_match_data: Dict, participant_puuid: str | None = None):
        self.participant_puuid = config.riot_api['puuid'] if participant_puuid is None else participant_puuid
        self.__parse_riot_match_data(riot_match_data)

    def __parse_participant(self, info_dto: Dict):
        if self.participant_puuid == 'any':
            self.participant = info_dto['participants'][0]
            return

        for participant in info_dto['participants']:
            if participant['puuid'] == self.participant_puuid:
                self.participant = participant
                return

        raise ParticipantNotFoundException(f"[!] The participant with puuid {self.participant_puuid:} was not found in the match data")

    def __parse_riot_match_data(self, riot_match_dto: Dict):
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
            self.__parse_participant(info_dto)
            self.participant.pop('challenges', None)
            self.participant.pop('perks', None)
            self.participant.pop('missions', None)

        except KeyError as e:
            raise KeyError(f"[!] The Riot Match DTO ({self.match_id}) is missing a required key: {e}")

    def get_keys(self) -> List[str]:
        return list(self.get_as_dict().keys())

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
