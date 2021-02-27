import psycopg2
import logging

from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from psycopg2 import sql

DBCONN_STR = "dbname='postgres' user='bhumika' host='34.87.112.231' password='doomsday13' connect_timeout=20"


class DBConn:

    def __init__(self, conn_str):
        logging.info('Connecting to db...')
        try:
            self.conn = psycopg2.connect(conn_str)
            logging.info('Connected to DB successfully.')
        except Exception as e:
            logging.exception(e)

    def execute(self, query):
        cur = self.conn.cursor()
        logging.debug(query.as_string(cur))

        try:
            cur.execute(query)
            self.conn.commit()
            cur.close()
            return True
        except Exception as e:
            logging.exception(e)
            cur.execute("rollback")
            cur.close()
            logging.exception('Unable to execute query')
            return False

    def fetch_all(self, query):
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        logging.debug(query.as_string(cur))
        cur.execute(query)
        rows =cur.fetchall()
        cur.close()
        return rows

    def fetch_one(self, query):
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        logging.debug(query.as_string(cur))
        cur.execute(query)
        rows =cur.fetchall()
        cur.close()
        return rows


db_conn = DBConn(DBCONN_STR)

def db():
    return db_conn