import os
import sys
import datetime
import importlib
import logging

from conn_adapter import ConnAdapter
from queue import Queue

__all__ = ['Worker']
_logger = logging.getLogger(__name__)


# A Worker object can process jobs from one or many queues.
class Worker(object):

    # Creates a new worker but does not start the worker. See Worker#start.
    # This method takes a single hash argument. The following keys are read:
    # fork_worker:: Worker forks each job execution.
    # wait_interval:: Time to wait between failed lock attempts
    # connection:: PGConn object.
    # q_name:: Name of a single queue to process.
    # q_names:: Names of queues to process. Will process left to right.
    # top_bound:: Offset to the head of the queue. 1 == strict FIFO.
    def __init__(self, fork_worker=None, wait_interval=None, connection=None,
                 q_name=None, q_names=None, top_bound=None):
        assert isinstance(connection, ConnAdapter)
        if fork_worker is None:
            fork_worker = bool(os.environ.get('QC_FORK_WORKER', ''))
        if wait_interval is None:
            wait_interval = int(os.environ.get('QC_LISTEN_TIME', '5'))
        self.fork_worker = fork_worker
        self.wait_interval = wait_interval
        self.conn_adapter = connection
        if q_name is None:
            q_name = os.environ.get('QUEUE', 'default')
        if q_names is None:
            q_names = os.environ.get('QUEUES', '')
            if not q_names:
                q_names = []
            else:
                q_names = q_names.split(',')
        self.queues = self.__setup_queues(
            self.conn_adapter, q_name, q_names, top_bound)
        self.running = True
        _logger.info("worker_initialized")

    # Commences the working of jobs.
    # start() spins on @running -which is initialized as true.
    # This method is the primary entry point to starting the worker.
    # The canonical example of starting a worker is as follows:
    # QC::Worker.new.start
    def start(self):
        while self.running:
            if self.fork_worker:
                self.fork_and_work()
            else:
                self.work()

    # Signals the worker to stop taking new work.
    # This method has no immediate effect. However, there are
    # two loops in the worker (one in #start and another in #lock_job)
    # which check the @running variable to determine if further progress
    # is desirable. In the case that @running is false, the aforementioned
    # methods will short circuit and cause the blocking call to #start
    # to unblock.
    def stop(self):
        self.running = False

    # Calls Worker#work but after the current process is forked.
    # The parent process will wait on the child process to exit.
    def fork_and_work(self):
        cpid = os.fork()
        if cpid == 0:
            self.setup_child()
            self.work()
            os._exit(0)
        else:
            _logger.info("fork pid=" + str(cpid))
            os.waitpid(cpid, 0)

    # Blocks on locking a job, and once a job is locked,
    # it will process the job.
    def work(self):
        queue, job = self.lock_job()
        if queue and job:
            _logger.info("work job=" + str(job['id']))
            self.process(queue, job)

    # Attempt to lock a job in the queue's table.
    # If a job can be locked, this method returns an array with
    # 2 elements. The first element is the queue from which the job was locked
    # and the second is a hash representation of the job.
    # If a job is returned, its locked_at column has been set in the
    # job's row. It is the caller's responsibility to delete the job row
    # from the table when the job is complete.
    def lock_job(self):
        _logger.info("lock_job")
        job = None
        while self.running:
            for queue in self.queues:
                job = queue.lock()
                if job:
                    return (queue, job)
            self.conn_adapter.wait(self.wait_interval,
                *[queue.name for queue in self.queues])

    # A job is processed by evaluating the target code.
    # if the job is evaluated with no exceptions
    # then it is deleted from the queue.
    # If the job has raised an exception the responsibility of what
    # to do with the job is delegated to Worker#handle_failure.
    # If the job is not finished and an INT signal is trapped,
    # this method will unlock the job in the queue.
    def process(self, queue, job):
        start = datetime.datetime.now()
        finished = False
        try:
            self.call(job)
            queue.delete(job['id'])
            finished = True
        except Exception, e:
            self.handle_failure(job, e)
            finished = True
        finally:
            if not finished:
                queue.unlock(job['id'])
            ttp = datetime.datetime.now() - start
            _logger.info("measure#qc.time-to-process=%s source=%s"
                         % (int(ttp.microseconds / 1000), queue.name))

    # Each job includes a method column. We will use ruby's eval
    # to grab the ruby object from memory. We send the method to
    # the object and pass the args.
    def call(self, job):
        args = job['args']
        receiver_str, _, message = job['method'].rpartition('.')
        module = importlib.import_module(receiver_str)
        getattr(module, message)(*args)

    # This method will be called when an exception
    # is raised during the execution of the job.
    def handle_failure(self, job, e):
        _logger.error("count#qc.job-error=1 job=%s error=%s"
                      % (repr(job), repr(e)))

    # This method should be overriden if
    # your worker is forking and you need to
    # re-establish database connections
    def setup_child(self):
        _logger.info("setup_child")

    def log(self, data):
        _logger.info(data)

    def __setup_queues(self, adapter, queue, queues, top_bound):
        names = (queues if len(queues) > 0 else [queue])
        return [Queue(adapter, name, top_bound) for name in names]
