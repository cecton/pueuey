import psycopg2
import select


class Transaction(object):
    def __init__(self, curs):
        self.curs = curs

    def __enter__(self):
        self.curs.execute("BEGIN")
        return self.curs

    def __exit__(self, *exc):
        self.curs.execute("ROLLBACK" if any(exc) else "COMMIT")


class Conn(object):
    def __init__(self, *args, **kwargs):
        cursor_kwargs = kwargs.pop('cursor', {})
        if len(args) == 1 and not kwargs and \
                isinstance(args[0], psycopg2.extensions.connection):
            self.conn = args[0]
            self.__setup__()
        else:
            self.conn = psycopg2.connect(*args, **kwargs)
            self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            self.curs = self.cursor(**cursor_kwargs)

    @property
    def dsn(self):
        return self.conn.dsn

    def disconnect(self):
        try:
            self.conn.close()
        finally:
            self.conn = None

    def execute(self, *args):
        self.curs.execute(*args)
        return self.curs

    def notify(self, chan):
        return self.curs.execute('NOTIFY "%s"' % chan)

    def listen(self, chan):
        return self.curs.execute('LISTEN "%s"' % chan)

    def unlisten(self, chan):
        return self.curs.execute('UNLISTEN "%s"' % chan)

    def drain_notify(self):
        notifies = list(self.conn.notifies)
        del self.conn.notifies[:]
        return notifies

    def wait_for_notify(self, t):
        if not any(select.select([self.conn], [], [], t)):
            return None
        else:
            self.conn.poll()
            if self.conn.notifies:
                return self.conn.notifies.pop()
        return None

    def transaction(self, *args, **kwargs):
        return Transaction(self.cursor(*args, **kwargs))

    def cursor(self, *args, **kwargs):
        return self.conn.cursor(*args, **kwargs)

    def transaction_idle(self):
        return self.conn.get_transaction_status
