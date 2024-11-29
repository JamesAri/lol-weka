import logging
import psycopg
import atexit

from db.executor import Executor
import config

logger = logging.getLogger(__name__)

# Create db connection
_pg_connection_dict = {
    'dbname': config.postgres['db_name'],
    'user': config.postgres['user'],
    'password': config.postgres['secret'],
    'port': config.postgres['port'],
    'host': 'localhost',
}

conn = psycopg.connect(**_pg_connection_dict)
cur = conn.cursor()
exec = Executor(conn, cur)

__all__ = ['cur', 'conn', 'exec']


def teardown():
    logger.warning("Database teardown in progress...")
    cur.close()
    conn.close()


atexit.register(teardown)
