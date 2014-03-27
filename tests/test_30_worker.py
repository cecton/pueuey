import os
import time
import random
import tempfile
import shutil
import unittest2
import subprocess

from pueuey import Queue, ConnAdapter, Worker
from common import Notifier, ConnBaseTest
import example_worker


reg_args = None

def register(*args):
    global reg_args
    reg_args = args

class WorkerTest(ConnBaseTest):
    q_name = 'test_worker'
    concurrent_workers = 10
    tasks = 100
    tries = 100

    def setUp(self):
        super(WorkerTest, self).setUp()
        self.working_directory = tempfile.mkdtemp()
        self.running_workers = []

    def _cleanup(self):
        for process in self.running_workers:
            process.terminate()
            process.wait()
        shutil.rmtree(self.working_directory)
        super(WorkerTest, self)._cleanup()

    def _invoke_worker(self, fork=False):
        p = subprocess.Popen(
            [example_worker.__file__, self.working_directory,
             '--dbname=' + self.dbname, '--queue=' + self.q_name] +
            (['--forkworker'] if fork else []))
        self.running_workers.append(p)
        return p

    def _check_exists(self, name, retry=2):
        for i in range(1, retry + 1):
            if os.path.exists(os.path.join(self.working_directory, name)):
                break
            time.sleep(i)
        else:
            self.fail("file `%s' does not exists!" % name)

    def test_00_fetch_something(self):
        global reg_args
        worker = Worker(connection=self._connect(), q_name=self.q_name)
        for i in range(self.tries):
            self.queue.enqueue("test_30_worker.register", ["foo", "bar"])
            worker.work()
            self.assertEqual(reg_args, ("foo", "bar"))
            reg_args = None

    def test_10_one_worker_success(self):
        self._invoke_worker()
        self.queue.enqueue("example_worker.touch", ["foo"])
        self._check_exists("foo")

    @unittest2.expectedFailure
    def test_15_one_worker_failure(self):
        self._invoke_worker()
        self.queue.enqueue("example_worker.touch", ["foo"])
        self._check_exists("bar")

    def test_20_lot_of_workers_no_delay(self):
        for i in range(self.concurrent_workers):
            self._invoke_worker()
        for i in range(self.tasks):
            self.queue.enqueue("example_worker.touch", ["job_%03d" % i])
        for i in range(self.tasks):
            self._check_exists("job_%03d" % i)
        self.assertEqual(self.queue.count(), 0)

    def test_25_lot_of_workers_random_delay(self):
        for i in range(self.concurrent_workers):
            self._invoke_worker()
        for i in range(self.tasks):
            self.queue.enqueue("example_worker.touch",
                ["job_%03d" % i, random.randint(0, 2)])
        for i in range(self.tasks):
            self._check_exists("job_%03d" % i, retry=5)
        time.sleep(2) # wait the maximum delay to make sure all jobs have ended
        self.assertEqual(self.queue.count(), 0)

    def test_30_lot_of_workers_no_delay_using_fork(self):
        for i in range(self.concurrent_workers):
            self._invoke_worker(fork=True)
        for i in range(self.tasks):
            self.queue.enqueue("example_worker.touch", ["job_%03d" % i])
        for i in range(self.tasks):
            self._check_exists("job_%03d" % i)
        self.assertEqual(self.queue.count(), 0)

    def test_35_lot_of_workers_random_delay_using_fork(self):
        for i in range(self.concurrent_workers):
            self._invoke_worker(fork=True)
        for i in range(self.tasks):
            self.queue.enqueue("example_worker.touch",
                ["job_%03d" % i, random.randint(0, 2)])
        for i in range(self.tasks):
            self._check_exists("job_%03d" % i, retry=5)
        time.sleep(2) # wait the maximum delay to make sure all jobs have ended
        self.assertEqual(self.queue.count(), 0)
