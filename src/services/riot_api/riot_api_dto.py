from typing import Any, Dict, List

import config
from errors import ParticipantNotFoundException

# https://developer.riotgames.com/apis#match-v5/GET_getMatch


class MatchDto:

    participant_puuid: str
    participant: Dict = None
    metadata: Dict = {}
    teams: Dict = {}

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

            # Match metadata
            self.metadata = {
                'match_id': metadata_dto['matchId'],
                'game_mode': info_dto['gameMode'],
                'game_creation': info_dto['gameCreation'],
                'game_start_timestamp': info_dto['gameStartTimestamp'],
                'game_end_timestamp': info_dto['gameEndTimestamp'],
                'game_duration': info_dto['gameDuration'],
            }

            # Participant data (match_dto is mapped 1:1 with one player)
            # NOTE: Participant data may differ based on game version
            self.__parse_participant(info_dto)
            self.participant.pop('challenges', None)
            self.participant.pop('perks', None)
            self.participant.pop('missions', None)

            # Teams data - only for PvP matches
            if len(info_dto['teams']) != 2:
                return

            team_id = self.participant['teamId']
            friendly_team = info_dto['teams'][0]
            enemy_team = info_dto['teams'][1]

            if friendly_team['teamId'] != team_id:
                friendly_team, enemy_team = enemy_team, friendly_team

            for objective, stats in friendly_team['objectives'].items():
                self.teams[f'friendly_{objective}_kills'] = stats['kills']
                self.teams[f'friendly_{objective}_first'] = stats['first']

            for objective, stats in enemy_team['objectives'].items():
                self.teams[f'enemy_{objective}_kills'] = stats['kills']
                self.teams[f'enemy_{objective}_first'] = stats['first']

        except KeyError as e:
            raise KeyError(f"[!] The Riot Match DTO ({self.metadata['match_id']}) is missing a required key: {e}")
        except ParticipantNotFoundException:
            raise
        except Exception as e:
            raise Exception(f"[!] An error occurred while parsing the Riot Match DTO ({self.metadata['match_id']}): {e}")

    def get_keys(self) -> List[str]:
        return list(self.get_as_dict().keys())

    def get_as_dict(self) -> Dict[str, Any]:
        return {
            **self.metadata,
            **self.participant,
            **self.teams,
        }
