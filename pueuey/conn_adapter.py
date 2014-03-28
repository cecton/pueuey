import os
import re
import psycopg2
import select
import urlparse

from log import log

__all__ = ['ConnAdapter']


class ConnAdapter(object):
    def __str__(self):
        return "<%s object at 0x%x; dsn: %s, closed: %d>" \
            % (self.__class__.__name__, id(self),
               repr(self.connection.dsn), self.connection.closed)

    def __init__(self, connection=None):
        self.connection = (
            self.__establish_new()
            if connection is None
            else self.__validate(connection)
        )
        self.connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def disconnect(self):
        try:
            return self.connection.close()
        except Exception, e:
            log(at='disconnect', error=repr(e))
            raise

    def execute(self, *args):
        log(at='exec_sql', sql=repr(args[0]))
        try:
            with self.connection.cursor() as curs:
                curs.execute(*args)
        except Exception, e:
            log(error=repr(e))
            raise

    def wait(self, time, *channels):
        curs = self.connection.cursor()
        listen_cmds = ['LISTEN "%s"' % c for c in channels]
        curs.execute(';'.join(listen_cmds))
        channel = self.__wait_for_notify(time)
        unlisten_cmds = ['UNLISTEN "%s"' % c for c in channels]
        curs.execute(';'.join(unlisten_cmds))
        self.__drain_notify()
        return channel

    def __drain_notify(self):
        notifies = list(self.connection.notifies)
        if notifies:
            log(at='drain_notifications')
            del self.connection.notifies[:]
        return notifies

    def __wait_for_notify(self, t):
        if any(select.select([self.connection], [], [], t)):
            self.connection.poll()
            if self.connection.notifies:
                return self.connection.notifies.pop()
        return None

    def __validate(self, connection):
        assert isinstance(connection, psycopg2._psycopg.connection), \
            "connection must be an instance of " \
            "psycopg2._psycopg.connection, but was " + repr(type(connection))
        return connection

    def __establish_new(self):
        log(at='establish_conn')
        try:
            conn = psycopg2.connect(**self.__normalize_db_url(self.__db_url()))
        except Exception, e:
            log(error=repr(e))
            raise
        with conn.cursor() as curs:
            curs.execute("SET application_name = %s",
                [os.environ.get('QC_APP_NAME', 'queue_classic')])
        return conn

    def __normalize_db_url(self, url):
        host = url.hostname
        if host:
            host = re.sub(r"%2F", "/", host, flags=re.I)

        return {
            'host': host, # host or percent-encoded socket path
            'port': url.port,
            'database': re.sub(r"/", "", url.path),
            'username' : url.username,
            'password' : url.password,
        }

    def __db_url(self):
        url = os.environ.get('QC_DATABASE_URL', os.environ.get('DATABASE_URL'))
        if not url:
            raise ValueError("missing QC_DATABASE_URL or DATABASE_URL")
        return urlparse.urlparse(url)
