from db import cur, conn, exec

def saveMatches(matches):
	query = "INSERT INTO matches (match_string) VALUES (%s)"
	exec.executemany(query=query, params_seq=matches)
	


def getAllMatches():
	cur.execute("SELECT * FROM matches")
	return cur.fetchall()
	




