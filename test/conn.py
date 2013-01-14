import os
import threading
from .common import Notifier, ConnBaseTest
from queue_classic import Conn

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
"SELECT * FROM %s" % self.table
+" WHERE q_name = %s AND method = %s",
["name_%02d" % i, "method_%02d" % i])
            for row in curs:
                self.assertEqual(row.q_name, "name_%02d" % i)
                self.assertEqual(row.method, "method_%02d" % i)

    def test_30_transaction_succeed(self):
        with self.conn.transaction() as curs:
            curs.execute(
"INSERT INTO %s" % self.table
+" (q_name, method) VALUES (%s, %s)",
["name_transaction", "method_transaction"])
        curs = self.conn.execute(
"SELECT * FROM %s" % self.table
+" WHERE q_name = %s AND method = %s",
["name_transaction", "method_transaction"])
        for row in curs:
            self.assertEqual(row.q_name, "name_transaction")
            self.assertEqual(row.method, "method_transaction")

    def test_35_transaction_failure(self):
        try:
            with self.conn.transaction() as curs:
                curs.execute(
"INSERT INTO %s" % self.table
+" (q_name, method) VALUES (%s, %s)",
["name_transaction", "method_transaction"])
                raise StandardError
        except StandardError:
            pass
        curs = self.conn.execute(
"SELECT * FROM %s" % self.table
+" WHERE q_name = %s AND method = %s",
["name_transaction_failure", "method_transaction_failure"])
        for row in curs:
            self.assertNotEqual(row.q_name, "name_transaction_failure")
            self.assertNotEqual(row.method, "method_transaction_failure")

    def test_40_wait_for_notify(self):
        for try_index in range(self.tries):
            self.conn.listen('test_chan')
            notifier = Notifier(self.Conn(), 'test_chan', 2)
            notifier.start()
            got = self.conn.wait_for_notify(4)
            self.assertEqual(got.channel, 'test_chan')
            notifier.join()
            rest = self.conn.drain_notify()
            self.assertTrue(len(rest) == 0)
            self.conn.unlisten('test_chan')

    def test_45_wait_for_notify_something_else(self):
        for try_index in range(self.tries):
            self.conn.listen('test_chan')
            notifier = Notifier(self.Conn(), 'something_else', 2)
            notifier.start()
            got = self.conn.wait_for_notify(4)
            self.assertTrue(got is None)
            notifier.join()
            rest = self.conn.drain_notify()
            self.assertTrue(len(rest) == 0)
            self.conn.unlisten('test_chan')

    def test_50_wait_for_notify_without_listen(self):
        notifier = Notifier(self.conn, 'something', 2)
        notifier.start()
        got = self.conn.wait_for_notify(5)
        self.assertTrue(got is None)
        notifier.join()
        rest = self.conn.drain_notify()
        self.assertTrue(len(rest) == 0)
