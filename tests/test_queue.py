import os
import threading
import psycopg2.extras

from pueuey import Queue, Conn
from .common import Notifier, ConnBaseTest


class ConcurrentClient(threading.Thread):
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
    lock = threading.RLock()

    def __init__(self, queue, stack):
        self.stack = stack
        super(ConcurrentLock, self).__init__(queue)

    def run(self):
        self.error = None
        self.connect()
        with self.lock:
            got = self.queue.lock(columns=['method','args','id'])
            if got is None:
                self.error = "Can't lock a new job!"
                return
            try:
                id = got.pop('id')
                i = self.stack.index(got)
                self.stack.pop(i)
                self.queue.delete(id)
            except ValueError:
                self.error = "Expected to find %s, in %s" % (got, self.stack)


class ConcurrentEnqueue(ConcurrentClient):
    def __init__(self, queue, job):
        self.job = job
        super(ConcurrentEnqueue, self).__init__(queue)

    def run(self):
        try:
            self.connect()
            self.queue.enqueue(**self.job)
        except BaseException, e:
            self.error = e


class QueueTest(ConnBaseTest):
    max_clients = 10
    tries = 100
    queues = 10
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

    def test_25_main_queue_multiple_connections(self):
        queues = []
        for i in range(self.queues):
            queues.append( Queue(
                Conn(self.db_queue.queries.conn.dsn,
                    cursor={'cursor_factory' : psycopg2.extras.RealDictCursor}),
                self.db_queue.queries.table,
                self.db_queue.name,
                self.db_queue.chan is not None) )

        stack = []
        for i in range(self.tries):
            queue = queues[i % len(queues)]
            job = {
                'method' : 'Kernel.puts',
                'args' : '["Job %d executed"]' % i,
            }
            job['id'] = queue.enqueue(**job)
            stack.append(job)

        for i in range(self.tries):
            queue = queues[i % len(queues)]
            got = self.db_queue.lock(columns=['id','method','args'])
            self.assertTrue(got is not None, "Can't pop on queue!")
            try:
                i = stack.index(got)
                stack.pop(i)
                # TODO: Problems appears if you don't delete the job
                queue.delete(got['id'])
            except ValueError:
                self.fail("Expected to find %s, in %s" % (got, stack))

    def test_30_multiple_queue_multiple_connections(self):
        queues = []
        for i in range(self.queues):
            name = "queue_%03d" % i
            queues.append( Queue(
                Conn(self.db_queue.queries.conn.dsn,
                    cursor={'cursor_factory' : psycopg2.extras.RealDictCursor}),
                self.db_queue.queries.table,
                name,
                self.db_queue.chan is not None) )

        stack = []
        for i in range(self.tries):
            queue = queues[i % len(queues)]
            job = {
                'method' : 'Kernel.puts',
                'args' : '["Job %d executed"]' % i,
            }
            job['id'] = queue.enqueue(**job)
            stack.append(job)

        for i in range(self.tries):
            queue = queues[i % len(queues)]
            got = queue.lock(columns=['id','method','args'])
            self.assertTrue(got is not None)
            try:
                i = stack.index(got)
                stack.pop(i)
                # TODO: Problems appears if you don't delete the job
                queue.delete(got['id']) 
            except ValueError:
                self.fail("Expected to find %s, in %s" % (got, stack))

    def test_35_main_queue_concurrent(self):
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
                if locker.error: self.fail(locker.error)
            lockers = lockers[self.max_clients:]

        self.assertTrue(len(stack) == 0,
            "The stack of jobs should be empty after locked them all")

    def test_40_multiple_queues_concurrent(self):
        queues = []
        stack = {}
        for i in range(self.queues):
            name = "queue_%03d" % i
            queues.append( Queue(
                Conn(self.db_queue.queries.conn.dsn,
                    cursor={'cursor_factory' : psycopg2.extras.RealDictCursor}),
                self.db_queue.queries.table,
                name,
                self.db_queue.chan is not None) )
            stack[name] = []

        enqueuers = []
        lockers = []
        for i in range(self.tries):
            for queue in queues:
                job = {
                    'method' : '%s_test_method_%03d' % (queue.name, i),
                    'args' : '%s_test_args_%03d' % (queue.name, i),
                }
                stack[queue.name].append(job)
                enqueuers.append(
                    ConcurrentEnqueue(queue, job))
                lockers.append(
                    ConcurrentLock(queue, stack[queue.name]))

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
                if locker.error: self.fail(locker.error)
            lockers = lockers[self.max_clients:]

        self.assertTrue(all(len(s) == 0 for s in stack.values()),
            "The stack of jobs should be empty after locked them all")
