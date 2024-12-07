class MatchDataNotFoundException(Exception):
    """Exception raised when match data is not found."""

    def __init__(self, message="Match data not found"):
        super().__init__(message)
