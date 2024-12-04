import logging

logger = logging.getLogger(__name__)


class Executor:
    """
    Execute the queries and commit the changes on success to the database,
    otherwise rollback the changes

    Closing a connection without committing the changes first will
    cause an implicit rollback to be performed - https://www.psycopg.org/docs/connection.html
    """

    def __init__(self, conn, cur):
        self.conn = conn
        self.cur = cur

    async def executemany(self, query, params_seq, returning=False):
        try:
            await self.cur.executemany(query=query, params_seq=params_seq, returning=returning)
            await self.conn.commit()
        except Exception as e:
            logger.critical(f"[!] An error occurred at executemany, rollback initiated")
            await self.conn.rollback()
            raise
