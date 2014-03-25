import psycopg2
import select

__all__ = ['ConnAdapter']


class ConnAdapter(object):
    def __str__(self):
        return "<%s object at 0x%x; dsn: %s, closed: %d>" \
            % (self.__class__.__name__, id(self),
               repr(self.connection.dsn), self.connection.closed)

    def __init__(self, connection):
        assert isinstance(connection, psycopg2._psycopg.connection)
        self.connection = connection
        self.connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def disconnect(self):
        return self.close()

    def execute(self, *args):
        curs = self.connection.cursor()
        curs.execute(*args)
        return curs

    def wait(self, time, *channels):
        listen_cmds = ['LISTEN "%s"' % c for c in channels]
        self.execute(';'.join(listen_cmds))
        channel = self.__wait_for_notify(time)
        unlisten_cmds = ['UNLISTEN "%s"' % c for c in channels]
        self.execute(';'.join(unlisten_cmds))
        self.__drain_notify()
        return channel

    def __drain_notify(self):
        notifies = list(self.connection.notifies)
        del self.connection.notifies[:]
        return notifies

    def __wait_for_notify(self, t):
        if any(select.select([self.connection], [], [], t)):
            self.connection.poll()
            if self.connection.notifies:
                return self.connection.notifies.pop()
        return None
