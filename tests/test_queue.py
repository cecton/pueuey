import os
import threading
import psycopg2.extras
import unittest

from pueuey import Queue, ConnAdapter
from common import Notifier, ConnBaseTest


class ConcurrentClient(threading.Thread):
    def __init__(self, queue):
        self.base_queue = queue
        super(ConcurrentClient, self).__init__()

    def connect(self):
        self.queue = Queue(
            ConnAdapter(self.base_queue.conn.dsn,
                cursor_factory=psycopg2.extras.RealDictCursor),
            self.base_queue.table,
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
            got = self.queue.lock(columns=['id', 'method', 'args'])
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
    base_dsn = {
        'cursor_factory': psycopg2.extras.RealDictCursor,
    }

    def test_20_main_queue(self):
        for i in range(self.tries):
            self.assertTrue(self.db_queue.lock() is None)
            job = {
                'method' : 'test_method_%03d' % i,
                'args' : 'test_args_%03d' % i,
            }
            self.db_queue.enqueue(**job)
            got = self.db_queue.lock(columns=['id', 'method', 'args'])
            job_id = got.pop('id')
            self.assertIsNotNone(got, "Can't pop on queue!")
            self.assertEqual(got, job)
            self.db_queue.delete(job_id)

    def test_25_main_queue_multiple_connections(self):
        queues = []
        for i in range(self.queues):
            queues.append( Queue(
                ConnAdapter(self.db_queue.conn.dsn,
                    cursor_factory=psycopg2.extras.RealDictCursor),
                self.db_queue.table,
                self.db_queue.name,
                self.db_queue.chan is not None) )

        stack = []
        for i in range(self.tries):
            queue = queues[i % len(queues)]
            job = {
                'method' : 'Kernel.puts',
                'args' : '["Job %d executed"]' % i,
            }
            queue.enqueue(**job)
            stack.append(job)

        for i in range(self.tries):
            queue = queues[i % len(queues)]
            got = self.db_queue.lock(columns=['id','method','args'])
            self.assertIsNotNone(got, "Can't pop on queue!")
            try:
                job_id = got.pop('id')
                stack.remove(got)
                # NOTE: problems appears if you don't delete the job
                queue.delete(job_id)
            except ValueError:
                self.fail("can't remove item %s from the stack" % repr(got))

    def test_30_multiple_queue_multiple_connections(self):
        queues = []
        for i in range(self.queues):
            name = "queue_%03d" % i
            queues.append( Queue(
                ConnAdapter(self.db_queue.conn.dsn,
                    cursor_factory=psycopg2.extras.RealDictCursor),
                self.db_queue.table,
                name,
                self.db_queue.chan is not None) )

        stack = []
        for i in range(self.tries):
            queue = queues[i % len(queues)]
            job = {
                'method' : 'Kernel.puts',
                'args' : '["Job %d executed"]' % i,
            }
            queue.enqueue(**job)
            stack.append(job)

        for i in range(self.tries):
            queue = queues[i % len(queues)]
            got = queue.lock(columns=['id','method','args'])
            self.assertIsNotNone(got)
            try:
                job_id = got.pop('id')
                stack.remove(got)
                # TODO: Problems appears if you don't delete the job
                queue.delete(job_id)
            except ValueError:
                self.fail("can't remove item %s in stack" % got)

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

        self.assertEqual(len(stack), 0,
            "The stack of jobs should be empty after locked them all")

    def test_40_multiple_queues_concurrent(self):
        queues = []
        stacks = {}
        for i in range(self.queues):
            name = "queue_%03d" % i
            queues.append( Queue(
                ConnAdapter(self.db_queue.conn.dsn,
                    cursor_factory=psycopg2.extras.RealDictCursor),
                self.db_queue.table,
                name,
                self.db_queue.chan is not None) )
            stacks[name] = []

        enqueuers = []
        lockers = []
        for i in range(self.tries):
            for queue in queues:
                job = {
                    'method' : '%s_test_method_%03d' % (queue.name, i),
                    'args' : '%s_test_args_%03d' % (queue.name, i),
                }
                stacks[queue.name].append(job)
                enqueuers.append(
                    ConcurrentEnqueue(queue, job))
                lockers.append(
                    ConcurrentLock(queue, stacks[queue.name]))

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

        for stack in stacks.values():
            self.assertEqual(len(stack), 0,
                "The stacks of jobs should be empty after locked them all")
