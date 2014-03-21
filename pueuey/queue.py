import os
import psycopg2

import setup

__all__ = ['Queue']


class Queue(object):
    def __init__(self, conn, table, name, notify='default'):
        if notify == 'default':
            notify = os.environ.get('QC_LISTENING_WORKER', True)
        self.conn = conn
        self.table = table
        self.name = name
        self.chan = (name if notify else None)

    def enqueue(self, **row):
        #TODO: convert dict values who are actual dicts to JSON?
        row = dict(row, q_name=self.name)
        curs = self.conn.cursor(cursor_factory=psycopg2.extensions.cursor)
        curs.execute(
            'INSERT INTO "%s" (%s)' % (self.table, ",".join(row.keys()))
            +" VALUES ("+",".join('%s' for r in row)+") RETURNING id", row.values())
        if self.chan: self.conn.notify(self.chan)
        return curs.fetchone()[0]

    def lock(self, top_bound=None, columns=None):
        if top_bound is None:
            top_bound = os.environ.get('QC_TOP_BOUND', 9)
        if columns is None:
            columns = '*'
        elif hasattr(columns, '__iter__'):
            columns = ", ".join('"%s"' % c for c in columns)
        curs = self.conn.execute(
            "SELECT %s FROM lock_head_%s(%%s, %%s)" % (columns, self.table),
            [self.name, top_bound])
        return curs.fetchone()

    def delete(self, id):
        return self.conn.execute(
            'DELETE FROM "%s" WHERE id = %d' % (self.table, id))

    def delete_all(self):
        return self.conn.execute(
            'DELETE FROM "%s" WHERE q_name = %%s' % self.table, self.name)

    def count(self):
        curs = self.conn.cursor(cursor_factory=psycopg2.extensions.cursor)
        curs.execute('SELECT COUNT(*) FROM "%s" WHERE q_name = %%s'
                     % self.table, self.name)
        return curs.fetchone()[0]
