import os
import io
import threading
import subprocess
from time import sleep
import psycopg2
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

def connect(dbname, host=None, port=None, username=None, cursor_factory=None):
    return psycopg2.connect(database=dbname,
        host=host, port=port, user=username, cursor_factory=cursor_factory)

class Notifier(threading.Thread):
    def __init__(self, connection, chan, delay):
        super(Notifier, self).__init__()
        self.connection, self.chan, self.delay = connection, chan, delay
        self.connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def run(self):
        sleep(self.delay)
        curs = self.connection.cursor()
        curs.execute('NOTIFY "%s"' % self.chan)

class ConnBaseTest(unittest.TestCase):
    address = {
        'host' : 'localhost',
        'port' : 5432,
        'username' : os.environ['USER'],
    }
    basename = 'test_pueuey'
    createdb = 'createdb'
    dropdb = 'dropdb'
    cursor_factory = None
    q_name = 'default'

    def _connect(self):
        return connect(self.dbname,
            **dict(self.address, cursor_factory=self.cursor_factory))

    def _createdb(self):
        run(self.createdb, self.dbname, **self.address)

    def _dropdb(self):
        run(self.dropdb, self.dbname, **self.address)

    def _cleanup(self):
        setup.drop(self.conn, close=True)
        self._dropdb()

    def setUp(self):
        self.dbname = "%s_%d" % (self.basename, id(self))
        self._createdb()
        self.conn = self._connect()
        setup.create(self.conn)
        self.addCleanup(self._cleanup)
        self.queue = Queue(self.conn, self.q_name)
