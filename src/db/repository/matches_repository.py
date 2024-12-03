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

    async def get_matches_older_than(self, cur, match_id: str | None) -> list[str]:
        """
        Get all matches older than the given match id. None for all matches.
        """
        if match_id is None:
            return await self.get_all_matches(cur)
        await cur.execute("SELECT match_string FROM matches WHERE match_string < %s", (match_id,))
        rows = await cur.fetchall()
        return [row[0] for row in rows]

    async def get_oldest_match(self, cur) -> str | None:
        """
        Get the oldest match id from the database.
        """
        await cur.execute("SELECT match_string FROM matches ORDER BY match_string ASC LIMIT 1")
        row = await cur.fetchone()
        return row[0] if row else None
