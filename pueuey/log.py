import datetime
import logging
from contextlib import contextmanager


_logger = logging.getLogger('pueuey')

def log(**data):
    data = dict(data, lib='pueuey')
    _logger.debug(" ".join([(k + '=' + v) for k, v in data.items()]))

@contextmanager
def log_yield(**data):
    try:
        t0 = datetime.datetime.now()
        yield
    except Exception, e:
        log(**dict(data, at='error', error=repr(e)))
        raise
    else:
        t = int((datetime.datetime.now() - t0).microseconds / 1000)
        log(**dict(data, elapsed=(str(t) + 'ms')))
