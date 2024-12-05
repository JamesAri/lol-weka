class MatchDataNotFoundException(Exception):
    """Exception raised when match data is not found."""

    def __init__(self, message="Match data not found"):
        super().__init__(message)


class ParticipantNotFoundException(Exception):
    """Exception raised when participant data is not found in match file. """

    def __init__(self, message="Participant data not found"):
        super().__init__(message)
