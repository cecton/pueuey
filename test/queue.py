import os
import threading
import psycopg2.extras
from queue_classic import Queue, Conn
from .common import Notifier, ConnBaseTest
from time import sleep


class ConcurrentClient(threading.Thread):
    lock = threading.RLock()
    asserts = []

    def __init__(self, queue):
        self.base_queue = queue
        super(ConcurrentClient, self).__init__()

    def connect(self):
        self.queue = Queue(
            Conn(self.base_queue.queries.conn.dsn,
                cursor={'cursor_factory' : psycopg2.extras.RealDictCursor}),
            self.base_queue.queries.table,
            self.base_queue.name,
            self.base_queue.chan is not None)


class ConcurrentLock(ConcurrentClient):
    def __init__(self, queue, stack):
        self.stack = stack
        super(ConcurrentLock, self).__init__(queue)

    def run(self):
        with self.lock:
            self.connect()
            for i in range(3):
                got = self.queue.lock(columns=['method','args','id'])
                if got is not None: break
                sleep(3)
            if got is None:
                self.__class__.asserts.append("Can't lock a new stack!")
            id = got.pop('id')
            if got not in self.stack: self.__class__.asserts.append(
                "Expected %s, got %s" % (self.stack, got))
            self.queue.delete(id)


class ConcurrentEnqueue(ConcurrentClient):
    def __init__(self, queue, job):
        self.job = job
        super(ConcurrentEnqueue, self).__init__(queue)

    def run(self):
        with self.lock:
            self.connect()
            self.queue.enqueue(**self.job)


class QueueTest(ConnBaseTest):
    max_clients = 10
    tries = 100
    base_dsn = dict(ConnBaseTest.base_dsn,
        cursor={'cursor_factory' : psycopg2.extras.RealDictCursor})

    def test_20_main_queue(self):
        for i in range(self.tries):
            self.assertTrue(self.db_queue.lock() is None)
            job = {
                'method' : 'test_method_%03d' % i,
                'args' : 'test_args_%03d' % i,
            }
            job['id'] = self.db_queue.enqueue(**job)
            got = self.db_queue.lock(columns=['id','method','args'])
            self.assertTrue(
                got is not None and
                got == job,
                "Expected %s, got %s" % (job, got))
            self.db_queue.delete(job['id'])

    def test_25_main_queue_concurrent(self):
        enqueuers = []
        lockers = []
        stack = []
        for i in range(self.tries):
            job = {
                'method' : 'test_method_%03d' % i,
                'args' : 'test_args_%03d' % i,
            }
            stack.append(job)
            enqueuers.append(
                ConcurrentEnqueue(self.db_queue, job))
            lockers.append(
                ConcurrentLock(self.db_queue, stack))

        while enqueuers:
            enqueuers_row = enqueuers[:self.max_clients]
            for enqueuer in enqueuers_row:
                enqueuer.start()
            for enqueuer in enqueuers_row:
                enqueuer.join()
            enqueuers = enqueuers[self.max_clients:]

        while lockers:
            lockers_row = lockers[:self.max_clients]
            for locker in lockers_row:
                locker.start()
            for locker in lockers_row:
                locker.join()
            lockers = lockers[self.max_clients:]
