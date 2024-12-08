from typing import Any, Dict, List

import config

# https://developer.riotgames.com/apis#match-v5/GET_getMatch


class MatchDto:

    participants: List[Dict] = None
    metadata: Dict = None
    team_data: Dict = None
    friendly_team: Dict = None
    enemy_team: Dict = None

    def __init__(self, riot_match_data: Dict):
        self.__parse_riot_match_data(riot_match_data)

    def __parse_riot_match_data(self, riot_match_dto: Dict):
        try:
            # Base data objects
            info_dto = riot_match_dto['info']
            metadata_dto = riot_match_dto['metadata']
            self.__parse_metadata(info_dto, metadata_dto)
            self.__parse_participants(info_dto)
            self.__parse_team_data(info_dto)
        except KeyError as e:
            raise KeyError(f"[!] The Riot Match DTO ({self.metadata['matchId']}) is missing a required key: {e}")
        except Exception as e:
            raise Exception(f"[!] An error occurred while parsing the Riot Match DTO ({self.metadata['matchId']}): {e}")

    def __parse_participants(self, info_dto: Dict):
        self.participants = info_dto['participants']
        for participant in info_dto['participants']:
            participant.pop('challenges', None)
            participant.pop('perks', None)
            participant.pop('missions', None)

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
        friendly_team_id = None
        for participant in info_dto['participants']:
            if friendly_team_id is not None:
                break
            for puuid in config.PUUIDS:
                if participant['puuid'] == puuid:
                    friendly_team_id = participant['teamId']
                    break

        friendly_team = info_dto['teams'][0]
        enemy_team = info_dto['teams'][1]

        if friendly_team['teamId'] != friendly_team_id:
            friendly_team, enemy_team = enemy_team, friendly_team

        self.friendly_team = friendly_team
        self.enemy_team = enemy_team

        self.__calculate_team_metrics(info_dto)

    def __calculate_team_metrics(self, info_dto):
        """ Calculates distance between teams metrics """
        items = info_dto['participants'][0].items()

        self.team_data = {}

        # Objectives
        for objective, stats in self.friendly_team['objectives'].items():
            self.team_data[f'team_{objective}_kills'] = stats['kills']
            self.team_data[f'team_{objective}_first'] = stats['first']
            if self.team_data.get(f'friendly_team_{objective}_kills', None) is None:
                self.team_data[f'friendly_team_{objective}_kills'] = 0
            self.team_data[f'friendly_team_{objective}_kills'] += stats['kills']

        for objective, stats in self.enemy_team['objectives'].items():
            self.team_data[f'team_{objective}_kills'] -= stats['kills']
            if self.team_data.get(f'enemy_team_{objective}_kills', None) is None:
                self.team_data[f'enemy_team_{objective}_kills'] = 0
            self.team_data[f'enemy_team_{objective}_kills'] += stats['kills']

        # Calculate distance between various metrics
        for key, value in items:
            if key != 'win':
                self.team_data[f"team_{key}"] = 0

        for participant in info_dto['participants']:
            for key, value in participant.items():
                # won't filter bools
                if not isinstance(value, (int, float, complex)):
                    continue
                if key not in participant:
                    # TODO: perhaps handle this better
                    continue

                if participant['teamId'] == self.friendly_team['teamId']:
                    if isinstance(value, bool):
                        if key != 'win':
                            self.team_data[f"team_{key}"] = bool(value if value else self.team_data[f"team_{key}"])
                        continue
                    self.team_data[f"team_{key}"] += value
                else:
                    if isinstance(value, bool):
                        continue
                    self.team_data[f"team_{key}"] -= value
