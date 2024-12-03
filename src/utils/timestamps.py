def get_next_timestamp(timestamp_ms: int) -> int:
    """
    We iterate the matches from the newest to the oldest.
    This function returns the timestamp of the next match to fetch.

    Timestamps returned from the Riot API are in milliseconds, but the
    MatchV5 API accepts seconds, so we need to convert them.

    @param timestamp_ms: timestamp in milliseconds (as retrieved from the API)
    @return: timestamp in seconds of the next match to fetch
    """
    # -1 s to avoid fetching the last match again
    return int(timestamp_ms / 1000) - 1
