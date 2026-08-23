"""dbt_runner package: patches multiprocessing before dbt is imported.

AWS Lambda provides no /dev/shm, so POSIX semaphores — what
multiprocessing locks are built on — fail with FileNotFoundError when
dbt registers its adapter. dbt only parallelizes with threads inside a
single process, so thread-level primitives are safe substitutes.
"""

import multiprocessing.context
import threading


def _rlock(self):
    return threading.RLock()


def _lock(self):
    return threading.Lock()


def _semaphore(self, value=1):
    return threading.Semaphore(value)


def _bounded_semaphore(self, value=1):
    return threading.BoundedSemaphore(value)


def _event(self):
    return threading.Event()


def _condition(self, lock=None):
    return threading.Condition(lock)


multiprocessing.context.BaseContext.RLock = _rlock
multiprocessing.context.BaseContext.Lock = _lock
multiprocessing.context.BaseContext.Semaphore = _semaphore
multiprocessing.context.BaseContext.BoundedSemaphore = _bounded_semaphore
multiprocessing.context.BaseContext.Event = _event
multiprocessing.context.BaseContext.Condition = _condition
