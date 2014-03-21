import os
import itertools
import psycopg2

import setup

__all__ = ['Queue']


class Queue(object):
    def __init__(self, conn, table, name):
        self.conn, self.table, self.name = conn, table, name

    def enqueue(self, **row):
        row = dict(row, q_name=self.name)
        curs = self.conn.cursor(cursor_factory=psycopg2.extensions.cursor)
        curs.execute(
            'INSERT INTO "%s" (%s) VALUES (%s) RETURNING id'
            % (self.table, ",".join(row.keys()),
               ",".join(itertools.repeat('%s', len(row)))),
            row.values())
        return curs.fetchone()[0] if curs.rowcount else None

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

    def unlock(self, id):
        return self.conn.execute(
            'UPDATE "%s" SET locked_at = NULL WHERE id = %%s' % self.table, id)

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
