class MatchesRepository:

    async def save_matches(self, exec, matches):
        query = "INSERT INTO matches (match_string) VALUES (%s)"
        await exec.executemany(query=query, params_seq=matches)

    async def get_all_matches(self, cur) -> list[str]:
        """
        Get all matches from the database.
        """
        await cur.execute("SELECT match_string FROM matches")
        rows = await cur.fetchall()
        return [row[0] for row in rows]

    async def get_oldest_match(self, cur) -> str | None:
        """
        Get the oldest match id from the database.
        """
        await cur.execute("SELECT match_string FROM matches ORDER BY match_string ASC LIMIT 1")
        row = await cur.fetchone()
        return row[0] if row else None
