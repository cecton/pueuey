import os
from .queries import Queries
from .setup import create_queue, drop_queue

__all__ = ['Queue']


class Queue(object):
    def __init__(self, conn, table, name, notify='default'):
        if notify == 'default':
            notify = os.environ.get('QC_LISTENING_WORKER', True)
        self.queries = Queries(conn, table)
        self.name = name
        self.chan = name if notify else None

    def enqueue(self, **row):
        return self.queries.insert(self.name, row, self.chan)

    def lock(self, top_bound=None, columns=None):
        return self.queries.lock_head(self.name, top_bound, columns)

    def delete(self, id):
        return self.queries.delete(id)

    def delete_all(self):
        return self.queries.delete_all(self.name)

    def count(self):
        return self.queries.count(self.name)

    def create(self, columns):
        return create_queue(self.queries.conn,
            table=self.queries.table, columns=columns)

    def drop(self):
        return drop_queue(self.queries.conn,
            table=self.queries.table)
