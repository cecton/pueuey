import os
import threading

from pueuey import ConnAdapter
from common import Notifier, ConnBaseTest

__all__ = ['ConnTest']


class ConnTest(ConnBaseTest):
    tries = 6

    def test_40_wait_for_notify(self):
        conn_adapter = ConnAdapter(self.conn)
        for try_index in range(self.tries):
            notifier = Notifier(self._connect(), 'test_chan', 0.1)
            notifier.start()
            got = conn_adapter.wait(0.5, 'test_chan')
            self.assertIsNotNone(got)
            self.assertEqual(got.channel, 'test_chan')
            notifier.join()

    def test_45_wait_for_notify_something_else(self):
        conn_adapter = ConnAdapter(self.conn)
        for try_index in range(self.tries):
            notifier = Notifier(self._connect(), 'something_else', 0.1)
            notifier.start()
            got = conn_adapter.wait(0.5, 'test_chan')
            self.assertIsNone(got)
            notifier.join()

    def test_50_wait_not_long_enough(self):
        conn_adapter = ConnAdapter(self.conn)
        notifier = Notifier(self._connect(), 'something', 1)
        notifier.start()
        got = conn_adapter.wait(0.1, 'something')
        self.assertIsNone(got)
        notifier.join()
