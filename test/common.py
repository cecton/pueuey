import os
import threading
from time import sleep
import unittest
from queue_classic import Conn, Queue

__all__ = ['Notifier', 'ConnBaseTest']


class Notifier(threading.Thread):
    def __init__(self, conn, chan, delay):
        super(Notifier, self).__init__()
        self.conn = conn
        self.chan = chan
        self.delay = delay

    def run(self):
        sleep(self.delay)
        self.conn.notify(self.chan)


class ConnBaseTest(unittest.TestCase):
    dbname = os.environ.get('QC_DBNAME', "ConnTest")
    table = 'queue_classic_jobs'
    q_name = 'default'
    base_dsn = {}

    def Conn(self, *args, **kwargs):
        return Conn(dbname=self.dbname, *args, **dict(self.base_dsn, **kwargs))

    @classmethod
    def setUpClass(cls):
        cls.conn = Conn(dbname="postgres", **cls.base_dsn)
        cls.conn.execute('CREATE DATABASE "%s";' % cls.dbname)
        cls.db_conn = Conn(dbname=cls.dbname, **cls.base_dsn)
        curs = cls.db_conn.execute("SELECT 1 FROM pg_language WHERE lanname = 'plpgsql';")
        if not curs.fetchone():
            cls.db_conn.execute("CREATE LANGUAGE plpgsql;")

    def setUp(self):
        self.db_queue = Queue(self.db_conn, self.table, self.q_name)
        self.db_queue.create("""\
  method varchar(255),
  args text""")
        self.conn = Conn(dbname=self.dbname, **self.base_dsn)

    def tearDown(self):
        self.conn.disconnect()
        self.db_queue.drop()

    @classmethod
    def tearDownClass(cls):
        cls.db_conn.disconnect()
        cls.conn.execute("DROP DATABASE \"%s\"" % cls.dbname)
        cls.conn.disconnect()
