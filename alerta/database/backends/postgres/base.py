import threading
import time
from collections import defaultdict, namedtuple
from datetime import datetime

from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool

from flask import current_app

from alerta.app import alarm_model
from alerta.database.base import Database
from alerta.exceptions import NoCustomerMatch
from alerta.models.enums import ADMIN_SCOPES
from alerta.models.heartbeat import HeartbeatStatus
from alerta.utils.format import DateTime
from alerta.utils.response import absolute_url

from .utils import Query

MAX_RETRIES = 5
class Backend(Database):

    def create_engine(self, app, uri, dbname=None, raise_on_error=True):
        self.uri = uri
        self.dbname = dbname

        lock = threading.Lock()
        with lock:
            conn = self.connect()
            with app.open_resource('sql/schema.sql') as f:
                try:
                    conn.cursor().execute(f.read())
                    conn.commit()
                except Exception as e:
                    if raise_on_error:
                        raise
                    app.logger.warning(e)

    def connect(self):
        retry = 0
        while True:
            try:
                pool = ConnectionPool(
                    conninfo=self.uri,
                    kwargs={
                        'dbname': self.dbname,
                        'client_encoding': 'UTF8',
                        'row_factory': namedtuple_row})

                # Test that we are able to connect to database
                with pool.connection(timeout=1.0):
                    self.logger.debug('Testing database connection')
                break
            except Exception as e:
                print(e)  # FIXME - should log this error instead of printing, but current_app is unavailable here
                retry += 1
                if retry > MAX_RETRIES:
                    conn = None
                    break
                else:
                    backoff = 2 ** retry
                    print(f'Retry attempt {retry}/{MAX_RETRIES} (wait={backoff}s)...')
                    time.sleep(backoff)

        if pool:
            return pool
        else:
            raise RuntimeError(f'Database connect error. Failed to connect after {MAX_RETRIES} retries.')

    @property
    def name(self):
        with self.pool.connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT current_database()')
            return cursor.fetchone()[0]

    @property
    def version(self):
        with self.pool.connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SHOW server_version')
            return cursor.fetchone()[0]

    @property
    def is_alive(self):
        with self.pool.connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT true')
            return cursor.fetchone()


    def destroy(self):
        with self.pool.connection() as conn:
            cursor = conn.cursor()
            for table in [
                'alerts',
                'blackouts',
                'customers',
                'groups',
                'heartbeats',
                'keys',
                'metrics',
                'perms',
                'users'
            ]:
                cursor.execute(f'DROP TABLE IF EXISTS {table} CASCADE')

    def truncate_table(self, table: str):
        with self.pool.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f'TRUNCATE TABLE {table} RESTART IDENTITY CASCADE')


    # SQL HELPERS
    def _insert(self, query, vars):
        """
        Insert, with return.
        """
        with self.pool.connection() as conn:
            cursor = conn.cursor()
            # self._log(cursor, query, vars)
            cursor.execute(query, vars)
            return cursor.fetchone()