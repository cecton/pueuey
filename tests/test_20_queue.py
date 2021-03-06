import os
import threading
import psycopg2
import psycopg2.extras
import unittest2

from pueuey import Queue, ConnAdapter
from common import Notifier, ConnBaseTest


class ConcurrentClient(threading.Thread):
    def __init__(self, queue):
        self.base_queue = queue
        super(ConcurrentClient, self).__init__()

    def connect(self):
        self.queue = Queue(self.base_queue.name)
        self.queue.conn_adapter = ConnAdapter(
            psycopg2.connect(self.base_queue.conn_adapter.connection.dsn))

class ConcurrentLock(ConcurrentClient):
    lock = threading.RLock()

    def __init__(self, queue, stack):
        self.stack = stack
        super(ConcurrentLock, self).__init__(queue)

    def run(self):
        self.error = None
        self.connect()
        with self.lock:
            got = self.queue.lock()
            if got is None:
                self.error = "Can't lock a new job!"
                return
            try:
                self.stack.remove((got['method'], got['args']))
                self.queue.delete(got['id'])
            except ValueError:
                self.error = "can not find %s in stack" % got

class ConcurrentEnqueue(ConcurrentClient):
    def __init__(self, queue, job):
        self.job = job
        super(ConcurrentEnqueue, self).__init__(queue)

    def run(self):
        try:
            self.connect()
            self.queue.enqueue(*self.job)
        except Exception, e:
            self.error = e


class QueueTest(ConnBaseTest):
    max_clients = 10
    tries = 100
    queues = 10
    cursor_factory = psycopg2.extras.RealDictCursor

    def test_20_main_queue(self):
        for i in range(self.tries):
            self.assertTrue(self.queue.lock() is None)
            job = {
                'method' : 'test_method_%03d' % i,
                'args' : [{'1': 'test_args_%03d' % i}],
            }
            self.queue.enqueue(job['method'], job['args'])
            got = self.queue.lock()
            self.assertIsNotNone(got)
            self.assertEqual(got['method'], job['method'])
            self.assertEqual(got['args'], job['args'])
            self.queue.delete(got['id'])

    def test_25_main_queue_multiple_connections(self):
        queues = []
        for i in range(self.queues):
            queue = Queue(self.queue.name)
            queue.conn_adapter = ConnAdapter(self._connect())
            queues.append(queue)

        stack = []
        for i in range(self.tries):
            queue = queues[i % len(queues)]
            job = {
                'method' : 'Kernel.puts',
                'args' : ["Job %d executed" % i],
            }
            queue.enqueue(job['method'], job['args'])
            stack.append(job)

        for i in range(self.tries):
            queue = queues[i % len(queues)]
            got = self.queue.lock()
            self.assertIsNotNone(got)
            try:
                stack.remove(dict(method=got['method'], args=got['args']))
                # NOTE: problems appears if you don't delete the job
                queue.delete(got['id'])
            except ValueError:
                self.fail("can't remove item %s from the stack" % repr(got))

    def test_30_multiple_queue_multiple_connections(self):
        queues = []
        for i in range(self.queues):
            queue = Queue("queue_%03d" % i)
            queue.conn_adapter = ConnAdapter(self._connect())
            queues.append(queue)

        stack = []
        for i in range(self.tries):
            queue = queues[i % len(queues)]
            job = ('Kernel.puts', ["Job %d executed" % i])
            queue.enqueue(*job)
            stack.append(job)

        for i in range(self.tries):
            queue = queues[i % len(queues)]
            got = queue.lock()
            self.assertIsNotNone(got)
            try:
                stack.remove((got['method'], got['args']))
                # TODO: Problems appears if you don't delete the job
                queue.delete(got['id'])
            except ValueError:
                self.fail("can't remove item %s in stack" % got)

    def test_35_main_queue_concurrent(self):
        enqueuers = []
        lockers = []
        stack = []
        for i in range(self.tries):
            job = ('test_method_%03d' % i, 'test_args_%03d' % i)
            stack.append(job)
            enqueuers.append(
                ConcurrentEnqueue(self.queue, job))
            lockers.append(
                ConcurrentLock(self.queue, stack))

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
            queue = Queue("queue_%03d" % i)
            queue.conn_adapter = ConnAdapter(self._connect())
            queues.append(queue)
            stacks[queue.name] = []

        enqueuers = []
        lockers = []
        for i in range(self.tries):
            for queue in queues:
                job = (
                    '%s_test_method_%03d' % (queue.name, i),
                    '%s_test_args_%03d' % (queue.name, i),
                )
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
