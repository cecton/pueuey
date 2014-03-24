import psycopg2
import select

__all__ = ['ConnAdapter']


class ConnAdapter(psycopg2._psycopg.connection):
    def __str__(self):
        return "<%s object at 0x%x; dsn: %s, closed: %d>" \
            % (self.__class__.__name__, id(self), repr(self.dsn), self.closed)

    def __init__(self, *args, **kwargs):
        if len(args) == 0:
            base_dsn = ''
        elif len(args) == 1:
            (base_dsn,) = args
        else:
            raise ValueError("unknown arguments: " + repr(args))

        async = kwargs.pop('async', False)
        self.connection_factory = kwargs.pop('connection_factory', None)
        self.cursor_factory = kwargs.pop('cursor_factory', None)

        if kwargs:
            dsn = ' '.join(
                ([base_dsn] if base_dsn else []) +
                ["%s=%s" % (k, psycopg2._param_escape(str(v)))
                 for (k, v) in kwargs.items() if v is not None])
        else:
            if not base_dsn:
                raise TypeError('missing dsn and no parameters')
            dsn = base_dsn

        super(self.__class__, self).__init__(dsn, async)

        self.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def disconnect(self):
        return self.close()

    def execute(self, *args):
        curs = self.cursor()
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
        notifies = list(self.notifies)
        del self.notifies[:]
        return notifies

    def __wait_for_notify(self, t):
        if any(select.select([self], [], [], t)):
            self.poll()
            if self.notifies:
                return self.notifies.pop()
        return None
