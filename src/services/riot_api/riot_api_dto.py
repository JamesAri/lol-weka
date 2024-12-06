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

    def __parse_riot_match_data(self, riot_match_dto: Dict):
        try:
            # Base data objects
            info_dto = riot_match_dto['info']
            metadata_dto = riot_match_dto['metadata']

            self.__parse_metadata(info_dto, metadata_dto)
            self.__parse_participant(info_dto)
            self.__parse_team_data(info_dto)
        except KeyError as e:
            raise KeyError(f"[!] The Riot Match DTO ({self.metadata['matchId']}) is missing a required key: {e}")
        except ParticipantNotFoundException:
            raise
        except Exception as e:
            raise Exception(f"[!] An error occurred while parsing the Riot Match DTO ({self.metadata['matchId']}): {e}")

    def __parse_participant(self, info_dto: Dict):
        # Participant data (match_dto is mapped 1:1 with one player)
        # NOTE: Participant data may differ based on game version
        if self.participant_puuid == 'any':
            self.participant = info_dto['participants'][0]
        else:
            for participant in info_dto['participants']:
                if participant['puuid'] == self.participant_puuid:
                    self.participant = participant
                    break

        if self.participant is None:
            raise ParticipantNotFoundException(f"[!] The participant with puuid {self.participant_puuid:} was not found in the match data")

        self.participant.pop('challenges', None)
        self.participant.pop('perks', None)
        self.participant.pop('missions', None)

    def __parse_metadata(self, info_dto: Dict, metadata_dto: Dict):
        # Match metadata
        self.metadata = {
            'matchId': metadata_dto['matchId'],
            'gameMode': info_dto['gameMode'],
            'gameCreation': info_dto['gameCreation'],
            'gameStartTimestamp': info_dto['gameStartTimestamp'],
            'gameEndTimestamp': info_dto['gameEndTimestamp'],
            'gameDuration': info_dto['gameDuration'],
        }

    def __parse_team_data(self, info_dto: Dict):
        # Teams data - only for PvP matches
        if len(info_dto['teams']) != 2:
            return

        # Merge participants into "team data"
        friendly_team_id = self.participant['teamId']
        friendly_team = info_dto['teams'][0]
        enemy_team = info_dto['teams'][1]

        if friendly_team['teamId'] != friendly_team_id:
            friendly_team, enemy_team = enemy_team, friendly_team

        for participant in info_dto['participants']:
            if participant['puuid'] == self.participant_puuid:
                continue

            for key in self.participant.keys():
                # won't filter bools
                if not isinstance(self.participant[key], (int, float, complex)):
                    continue
                if key not in participant:
                    continue

                if participant['teamId'] == friendly_team_id:
                    if isinstance(self.participant[key], bool):
                        if key != 'win':
                            self.participant[key] = participant[key] if participant[key] else self.participant[key]
                        continue
                    self.participant[key] += participant[key]

                else:
                    if isinstance(self.participant[key], bool):
                        continue
                    self.participant[key] -= participant[key]

        for objective, stats in friendly_team['objectives'].items():
            self.teams[f'{objective}_kills'] = stats['kills']
            self.teams[f'{objective}_first'] = stats['first']

        for objective, stats in enemy_team['objectives'].items():
            self.teams[f'{objective}_kills'] -= stats['kills']

    def get_keys(self) -> List[str]:
        return list(self.get_as_dict().keys())

    def get_as_dict(self) -> Dict[str, Any]:
        return {
            **self.metadata,
            **self.participant,
            **self.teams,
        }
