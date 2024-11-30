import logging
import psycopg

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


async def init_db():
    conn = await psycopg.AsyncConnection.connect(**_pg_connection_dict)
    cur = conn.cursor()
    exec = Executor(conn, cur)

    async def teardown():
        logger.info("[-] Database teardown in progress...")
        if not cur.closed:
            await cur.close()
        else:
            logger.warning("[!] Database cursor is already closed.")
        if not conn.closed:
            await conn.close()
        else:
            logger.warning("[!] Database connection is already closed.")

    return conn, cur, exec, teardown

__all__ = ['init_db']
