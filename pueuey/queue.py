import os
import sys
import itertools
import datetime
import psycopg2
import json

import setup

__all__ = ['Queue']


DEBUG = bool(os.environ.get('DEBUG', ''))

class Queue(object):
    def __init__(self, conn, name, topbound=None):
        self.conn, self.name = conn, name
        if topbound is None:
            self.topbound = os.environ.get('QC_TOP_BOUND', 9)
        else:
            self.top_bound = topbound

    def enqueue(self, method, args):
        args = json.dumps(args)
        curs = self.conn.cursor(cursor_factory=psycopg2.extensions.cursor)
        curs.execute(
            'INSERT INTO "queue_classic_jobs" (q_name, method, args) '
            'VALUES (%s, %s, %s) RETURNING id', [self.name, method, args])
        return curs.fetchone()[0]

    def lock(self, top_bound=None):
        if top_bound is None:
            top_bound = self.topbound
        curs = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        curs.execute(
            "SELECT * FROM lock_head(%s, %s)", [self.name, top_bound])
        if not curs.rowcount:
            return None
        job = curs.fetchone()
        # NOTE: JSON in args is parsed automatically
        #       timestamptz columns are converted automatically to datetime
        if job['created_at'] and DEBUG:
            now = datetime.datetime.now(job['created_at'].tzinfo)
            ttl = now - job['created_at']
            sys.stdout.write("measure#qc.time-to-lock=#%sms source=#%s\n"
                % (int(ttl.microseconds / 1000), self.name))
        return job

    def unlock(self, id):
        return self.conn.execute(
            'UPDATE "queue_classic_jobs" '
            'SET locked_at = NULL WHERE id = %s', [id])

    def delete(self, id):
        return self.conn.execute(
            'DELETE FROM "queue_classic_jobs" WHERE id = %s', [id])

    def delete_all(self):
        return self.conn.execute(
            'DELETE FROM "queue_classic_jobs" WHERE q_name = %s', [self.name])

    def count(self):
        curs = self.conn.cursor(cursor_factory=psycopg2.extensions.cursor)
        curs.execute('SELECT COUNT(*) FROM "queue_classic_jobs" '
                     'WHERE q_name = %s', [self.name])
        return curs.fetchone()[0]
