import os
import sys
import itertools
import datetime
import logging
import psycopg2
import psycopg2.extras
import json

from conn_adapter import ConnAdapter
import setup

__all__ = ['Queue']


_logger = logging.getLogger(__name__)

# The queue class maps a queue abstraction onto a database table.
class Queue(object):
    def __init__(self, name, top_bound=None):
        if top_bound is None:
            top_bound = os.environ.get('QC_TOP_BOUND', 9)
        self.name, self.top_bound = name, top_bound

    @property
    def conn_adapter(self):
        if not hasattr(self, '_adapter'):
            self._adapter = ConnAdapter()
        return self._adapter

    @conn_adapter.setter
    def conn_adapter(self, conn_adapter):
        self._adapter = conn_adapter

    # enqueue(m,a) inserts a row into the jobs table and trigger a notification
    # The job's queue is represented by a name column in the row.
    # There is a trigger on the table which will send a NOTIFY event
    # on a channel which corresponds to the name of the queue.
    # The method argument is a string encoded ruby expression. The expression
    # will be separated by a `.` character and then `eval`d.
    # Examples of the method argument include: `puts`, `Kernel.puts`,
    # `MyObject.new.puts`.
    # The args argument will be encoded as JSON and stored as a JSON datatype
    # in the row. (If the version of PG does not support JSON,
    # then the args will be stored as text.
    # The args are stored as a collection and then splatted inside the worker.
    # Examples of args include: `'hello world'`, `['hello world']`,
    # `'hello', 'world'`.
    def enqueue(self, method, args):
        args = json.dumps(args)
        curs = self.conn_adapter.connection\
            .cursor(cursor_factory=psycopg2.extensions.cursor)
        curs.execute(
            'INSERT INTO "queue_classic_jobs" (q_name, method, args) '
            'VALUES (%s, %s, %s) RETURNING id', [self.name, method, args])
        return curs.fetchone()[0]

    def lock(self, top_bound=None):
        if top_bound is None:
            top_bound = self.top_bound
        curs = self.conn_adapter.connection\
            .cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        curs.execute(
            "SELECT * FROM lock_head(%s, %s)", [self.name, top_bound])
        if not curs.rowcount:
            return None
        job = curs.fetchone()
        # NOTE: JSON in args is parsed automatically
        #       timestamptz columns are converted automatically to datetime
        if job['created_at']:
            now = datetime.datetime.now(job['created_at'].tzinfo)
            ttl = now - job['created_at']
            logging.info("measure#qc.time-to-lock=%sms source=%s"
                         % (int(ttl.microseconds / 1000), self.name))
        return job

    def unlock(self, id):
        return self.conn_adapter.execute(
            'UPDATE "queue_classic_jobs" '
            'SET locked_at = NULL WHERE id = %s', [id])

    def delete(self, id):
        return self.conn_adapter.execute(
            'DELETE FROM "queue_classic_jobs" WHERE id = %s', [id])

    def delete_all(self):
        return self.conn_adapter.execute(
            'DELETE FROM "queue_classic_jobs" WHERE q_name = %s', [self.name])

    def count(self):
        curs = self.conn_adapter.connection\
            .cursor(cursor_factory=psycopg2.extensions.cursor)
        curs.execute('SELECT COUNT(*) FROM "queue_classic_jobs" '
                     'WHERE q_name = %s', [self.name])
        return curs.fetchone()[0]
