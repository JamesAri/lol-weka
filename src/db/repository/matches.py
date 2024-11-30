async def saveMatches(exec, matches):
    query = "INSERT INTO matches (match_string) VALUES (%s)"
    await exec.executemany(query=query, params_seq=matches)


async def getAllMatches(cur):
    """
    Get all matches from the database.
    """
    await cur.execute("SELECT * FROM matches")
    return await cur.fetchall()
