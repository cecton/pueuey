import os
import threading

from common import Notifier, ConnBaseTest

__all__ = ['ConnTest']


class ConnTest(ConnBaseTest):
    tries = 6

    def test_25_execute(self):
        for i in range(self.tries):
            self.conn.execute(
"INSERT INTO %s (q_name, method)" % self.table
+" VALUES (%s, %s)",
["name_%02d" % i, "method_%02d" % i])
        for i in range(self.tries):
            curs = self.conn.execute(
"SELECT q_name, method FROM %s" % self.table
+" WHERE q_name = %s AND method = %s",
["name_%02d" % i, "method_%02d" % i])
            for q_name, method in curs:
                self.assertEqual(q_name, "name_%02d" % i)
                self.assertEqual(method, "method_%02d" % i)

    def test_40_wait_for_notify(self):
        for try_index in range(self.tries):
            notifier = Notifier(self._connect(), 'test_chan', 0.1)
            notifier.start()
            got = self.conn.wait(0.5, 'test_chan')
            self.assertIsNotNone(got)
            self.assertEqual(got.channel, 'test_chan')
            notifier.join()

    def test_45_wait_for_notify_something_else(self):
        for try_index in range(self.tries):
            notifier = Notifier(self._connect(), 'something_else', 0.1)
            notifier.start()
            got = self.conn.wait(0.5, 'test_chan')
            self.assertIsNone(got)
            notifier.join()

    def test_50_wait_not_long_enough(self):
        notifier = Notifier(self.conn, 'something', 1)
        notifier.start()
        got = self.conn.wait(0.1, 'something')
        self.assertIsNone(got)
        notifier.join()
