#!/usr/bin/env python2

import argparse
import os
import sys
import errno
import time
import psycopg2
import logging

import pueuey
from pueuey import Worker


def touch(name, sleep=0):
    time.sleep(sleep)
    logging.info("touch `%s'", name)
    fh = open(name, 'wb')
    fh.close()

class MyWorker(Worker):
    def __init__(self, working_directory, **kwargs):
        self.working_directory = working_directory
        super(MyWorker, self).__init__(**kwargs)

    def call(self, job):
        origin_directory = os.getcwd()
        os.chdir(self.working_directory)
        try:
            if self.fork_worker:
                logging.debug("PID/PPID: %d/%d", os.getpid(), os.getppid())
            super(MyWorker, self).call(job)
        finally:
            os.chdir(origin_directory)

    def handle_failure(self, job, e):
        super(MyWorker, self).handle_failure(job, e)
        logging.error("dsn: " + self.conn_adapter.connection.dsn)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--help', action='store_true')
parser.add_argument('--dbname', '-d', dest='dbname', type=str)
parser.add_argument('--host', '-h',
    help="Specifies the TCP port or the local Unix-domain socket file.")
parser.add_argument('--username', '-U',
    help="Connect to the database as the user username instead "
         "of the default.")
parser.add_argument('--port', '-p', type=int)
parser.add_argument('--queue', default='example_worker')
parser.add_argument('--forkworker', action='store_true', default=None)
parser.add_argument('--debug', action='store_true')
parser.add_argument('working_directory', nargs='?',
    help="Working directory to monitor the events (required)")

def run(args):
    if args.help or not args.working_directory:
        parser.print_help()
        sys.exit(0)

    if args.debug or os.environ.get('DEBUG', ''):
        logging.basicConfig(level=logging.DEBUG)

    if args.dbname:
        conn = psycopg2.connect(database=args.dbname,
            host=args.host, port=args.port, username=args.username)
    else:
        conn = None

    if args.forkworker:
        import signal
        signal.signal(signal.SIGTERM, lambda *a: os.kill(0, signal.SIGTERM))
        signal.signal(signal.SIGQUIT, lambda *a: os.kill(0, signal.SIGQUIT))

    worker = MyWorker(args.working_directory,
        connection=conn, q_name=args.queue, fork_worker=args.forkworker)
    try:
        worker.start()
    except OSError, exc:
        if exc.errno == errno.EINTR:
            sys.exit(0)
        else:
            raise
    sys.exit(1)

if __name__ == '__main__':
    args = parser.parse_args()
    run(args)
