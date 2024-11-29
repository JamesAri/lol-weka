import logging

logger = logging.getLogger(__name__)


# Execute the queries and commit the changes on success to the database,
# otherwise rollback the changes.
class Executor:
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur = cur

    def executemany(self, query, params_seq, returning=False):
        try:
            self.cur.executemany(query=query, params_seq=params_seq, returning=returning)
            self.conn.commit()
        except Exception as e:
            logger.critical(f"[!] An error occurred at executemany, rollback initiated")
            self.conn.rollback()
            raise
            
