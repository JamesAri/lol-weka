from typing import Dict

# https://developer.riotgames.com/apis#match-v5/GET_getMatch


class MatchDto:
    def __init__(self, riot_match_dto: Dict):
        self.__parse_riot_match_dto(riot_match_dto)

    @staticmethod
    def from_riot_match_dto(riot_match_dto: Dict) -> 'MatchDto':
        return MatchDto(
            riot_match_dto=riot_match_dto
        )

    def __parse_riot_match_dto(self, riot_match_dto: Dict):
        info_dto = riot_match_dto['info']
        metadata_dto = riot_match_dto['metadata']

        self.match_id = metadata_dto['matchId']
        self.end_of_game_result = info_dto['endOfGameResult']
        self.game_mode = info_dto['gameMode']
        self.game_creation = info_dto['gameCreation']
        self.game_start_timestamp = info_dto['gameStartTimestamp']
        self.game_end_timestamp = info_dto['gameEndTimestamp']
        self.game_duration = info_dto['gameDuration']
        pass
