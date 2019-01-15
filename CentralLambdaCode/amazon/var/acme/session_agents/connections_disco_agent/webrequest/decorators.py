import random
import sys
import time

from functools import wraps

def nop(val):
    return val

def retry(ExceptionToCheck, retries=4, delay=3, backoff=2, logger=None, handler=nop, excptHandler=nop):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param retries: number of times to retry before giving up
    :type retries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """

    if logger:
        logFn = logger.warning
    else:
        logFn = sys.stdout.write

    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            retryNum = 0
            maxDelay = delay
            while retryNum <= retries:
                try:
                    return handler(f(*args, **kwargs))
                except ExceptionToCheck as e:
                    e = excptHandler(e)
                    randDelay = random.uniform(0, maxDelay)
                    if retryNum < retries:
                        # The {:.4g} specifies to show up to 4 significant digits, see:
                        # https://docs.python.org/2/library/string.html#format-specification-mini-language
                        # Strings on different lines inside brackets are concatenated automatically:
                        # http://stackoverflow.com/questions/10660435/pythonic-way-to-create-a-long-multi-line-string
                        msg = ("{0}, Retrying in {1:.4g} seconds. Try {2} of {3}."
                               " Range for random sleep was 0s-{4:.4g}s"
                               .format(e, randDelay, retryNum+1, retries, maxDelay))
                        logFn(msg)
                    else:
                        msg = "{0}, Max retries of {1} reached, re-raising exception".format(e, retries)
                        logFn(msg)
                        raise
                    time.sleep(randDelay)
                    retryNum += 1
                    maxDelay *= backoff

        return f_retry  # true decorator

    return deco_retry

def synchronized(f):
    deco = synchronizedWith()
    return deco(f)

def synchronizedWith(lockField="_lock"):
    """
    Works with instance methods only with classes with "lock" fields containing
    a reentrant lock
    From: http://stackoverflow.com/a/4625483
    """

    def deco_sync(f):
        @wraps(f)
        def new_method(self, *args, **kwargs):
            lock = getattr(self, lockField)
            with lock:
                return f(self, *args, **kwargs)

        return new_method

    return deco_sync

def synchronizedNoBlocking(lockField="_lock", default=None):
    def deco(f):
        @wraps(f)
        def new_method(self, *args, **kwargs):
            lock = getattr(self, lockField)
            hasLock = lock.acquire(False)
            if hasLock:
                try:
                    return f(self, *args, **kwargs)
                finally:
                    lock.release()
            else:
                logger = getattr(self, 'logger', None)
                if logger is not None:
                    logger.debug("Call to {0} avoided as lock was already held".format(f.__name__))
                return default

        return new_method

    return deco

# Decorator for static variables in functions: http://stackoverflow.com/a/279586/583620
# From: http://stackoverflow.com/questions/279561/what-is-the-python-equivalent-of-static-variables-inside-a-function
def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func
    return decorate
