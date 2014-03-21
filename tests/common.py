import os
import io
import threading
import subprocess
from time import sleep
import unittest

from pueuey import ConnAdapter, Queue, setup

if not hasattr(subprocess, 'DEVNULL'):
    subprocess.DEVNULL = io.open(os.devnull, 'wb')

__all__ = ['Notifier', 'ConnBaseTest']


def run(command, *a, **kw):
    stdin = kw.pop('stdin', None)
    return subprocess.check_output(
        [command] +
        [(("-" if len(k) == 1 else "--") + str(k) + "=" + str(v))
         for k, v in kw.items() if v is not None] +
        list(a), stderr=subprocess.DEVNULL, stdin=stdin)

class Notifier(threading.Thread):
    def __init__(self, pg_conn, chan, delay):
        super(Notifier, self).__init__()
        self.pg_conn = pg_conn
        self.chan = chan
        self.delay = delay

    def run(self):
        sleep(self.delay)
        self.pg_conn.execute('NOTIFY "%s"' % self.chan)

class ConnBaseTest(unittest.TestCase):
    address = {
        'host' : 'localhost',
        'port' : 5432,
        'user' : os.environ['USER'],
    }
    basename = 'test_pueuey'
    createdb = 'createdb'
    dropdb = 'dropdb'
    cursor_factory = None
    table = 'queue_classic_jobs'
    q_name = 'default'

    def _connect(self, **kwargs):
        username = kwargs.pop('username', None)
        return ConnAdapter(**dict(kwargs,
            database=self.dbname, user=username))

    def _createdb(self):
        run(self.createdb, self.dbname, **self.address)

    def _dropdb(self):
        run(self.dropdb, self.dbname, **self.address)

    def _cleanup(self):
        setup.drop(self.conn, self.table, close=True)
        self._dropdb()

    def setUp(self):
        self.dbname = "%s_%d" % (self.basename, id(self))
        self._createdb()
        self.conn = ConnAdapter(dbname=self.dbname,
            cursor_factory=self.cursor_factory, **self.address)
        curs = self.conn.execute(
            "SELECT 1 FROM pg_language WHERE lanname = 'plpgsql'")
        if not curs.fetchone():
            self.conn.execute("CREATE LANGUAGE plpgsql")
        setup.create(self.conn, self.table,
            "method varchar(255), args text")
        self.addCleanup(self._cleanup)
        self.db_queue = Queue(self.conn, self.table, self.q_name)
