import psycopg2
import select

__all__ = ['ConnAdapter']


class ConnAdapter(psycopg2._psycopg.connection):
    def __str__(self):
        return "<%s object at 0x%x; dsn: %s, closed: %d>" \
            % (self.__class__.__name__, id(self), repr(self.dsn), self.closed)

    def __new__(cls, *args, **kwargs):
        return super(cls, cls).__new__(cls, *args, **kwargs)

    def __init__(self, *a, **kw):
        template = psycopg2.connect(*a, **kw)
        super(self.__class__, self).__init__(template.dsn, template.async)
        self.connection_factory = kw.get('connection_factory')
        self.cursor_factory = kw.get('cursor_factory')
        self.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def disconnect(self):
        return self.close()

    def execute(self, *args):
        curs = self.cursor()
        curs.execute(*args)
        return curs

    def notify(self, chan):
        return self.execute('NOTIFY "%s"' % chan)

    def listen(self, chan):
        return self.execute('LISTEN "%s"' % chan)

    def unlisten(self, chan):
        return self.execute('UNLISTEN "%s"' % chan)

    def __drain_notify(self):
        notifies = list(self.notifies)
        del self.notifies[:]
        return notifies

    def __wait_for_notify(self, t):
        if any(select.select([self], [], [], t)):
            self.poll()
            if self.notifies:
                return self.notifies.pop()
        return None
