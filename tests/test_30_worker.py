import tempfile

from pueuey import Queue, ConnAdapter, Worker
from common import Notifier, ConnBaseTest


def register(*args):
    global reg_args
    reg_args = args

class WorkerTest(ConnBaseTest):
    def test_10_fetch_something(self):
        global reg_args
        worker = Worker(connection=self._connect(), q_name=self.q_name)
        self.queue.enqueue("test_30_worker.register", ["foo", "bar"])
        worker.work()
        self.assertEqual(reg_args, ("foo", "bar"))
