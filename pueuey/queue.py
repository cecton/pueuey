import os
import sys
import itertools
import datetime
import logging
import psycopg2
import psycopg2.extras
import json

import setup

__all__ = ['Queue']


_logger = logging.getLogger(__name__)

class Queue(object):
    def __init__(self, conn, name, top_bound=None):
        assert isinstance(conn, psycopg2._psycopg.connection)
        if top_bound is None:
            top_bound = os.environ.get('QC_TOP_BOUND', 9)
        self.conn, self.name, self.top_bound = conn, name, top_bound
        self.conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def enqueue(self, method, args):
        args = json.dumps(args)
        curs = self.conn.cursor(cursor_factory=psycopg2.extensions.cursor)
        curs.execute(
            'INSERT INTO "queue_classic_jobs" (q_name, method, args) '
            'VALUES (%s, %s, %s) RETURNING id', [self.name, method, args])
        return curs.fetchone()[0]

    def lock(self, top_bound=None):
        if top_bound is None:
            top_bound = self.top_bound
        curs = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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
        curs = self.conn.cursor()
        return curs.execute(
            'UPDATE "queue_classic_jobs" '
            'SET locked_at = NULL WHERE id = %s', [id])

    def delete(self, id):
        curs = self.conn.cursor()
        return curs.execute(
            'DELETE FROM "queue_classic_jobs" WHERE id = %s', [id])

    def delete_all(self):
        curs = self.conn.cursor()
        return curs.execute(
            'DELETE FROM "queue_classic_jobs" WHERE q_name = %s', [self.name])

    def count(self):
        curs = self.conn.cursor(cursor_factory=psycopg2.extensions.cursor)
        curs.execute('SELECT COUNT(*) FROM "queue_classic_jobs" '
                     'WHERE q_name = %s', [self.name])
        return curs.fetchone()[0]
