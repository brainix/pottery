# --------------------------------------------------------------------------- #
#   redlock.py                                                                #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #
'''Distributed Redis-powered lock.

This algorithm safely and reliably provides a mutually-exclusive locking
primitive to protect a resource shared across threads, processes, and even
machines, without a single point of failure.

Rationale and algorithm description:
    http://redis.io/topics/distlock

Reference implementations:
    https://github.com/antirez/redlock-rb
    https://github.com/SPSCommerce/redlock-py

Lua scripting:
    https://github.com/andymccurdy/redis-py#lua-scripting
'''


import concurrent.futures
import contextlib
import os
import random
import time

from redis.exceptions import ConnectionError
from redis.exceptions import TimeoutError

from .base import Primitive
from .exceptions import ReleaseUnlockedLock
from .exceptions import TooManyExtensions
from .timer import ContextTimer


class Redlock(Primitive):
    '''Distributed Redis-powered lock.

    This algorithm safely and reliably provides a mutually-exclusive locking
    primitive to protect a resource shared across threads, processes, and even
    machines, without a single point of failure.

    Rationale and algorithm description:
        http://redis.io/topics/distlock

    Usage:

        >>> printer_lock = Redlock(key='printer')
        >>> bool(printer_lock.locked())
        False
        >>> printer_lock.acquire()
        True
        >>> bool(printer_lock.locked())
        True
        >>> # Critical section - print stuff here.
        >>> printer_lock.release()
        >>> bool(printer_lock.locked())
        False

    Redlocks time out (by default, after 10 seconds).  You should take care to
    ensure that your critical section completes well within the timeout.  The
    reasons that Redlocks time out are to preserve "liveness"
    (http://redis.io/topics/distlock#liveness-arguments) and to avoid deadlocks
    (in the event that a process dies inside a critical section before it
    releases its lock).

        >>> printer_lock.acquire()
        True
        >>> bool(printer_lock.locked())
        True
        >>> # Critical section - print stuff here.
        >>> time.sleep(10)
        >>> bool(printer_lock.locked())
        False

    If 10 seconds isn't enough to complete executing your critical section,
    then you can specify your own timeout:

        >>> printer_lock = Redlock(key='printer', auto_release_time=15*1000)
        >>> printer_lock.acquire()
        True
        >>> bool(printer_lock.locked())
        True
        >>> # Critical section - print stuff here.
        >>> time.sleep(10)
        >>> bool(printer_lock.locked())
        True
        >>> time.sleep(5)
        >>> bool(printer_lock.locked())
        False

    You can use a Redlock as a context manager:

        >>> states = []
        >>> with Redlock(key='printer') as printer_lock:
        ...     states.append(bool(printer_lock.locked()))
        ...     # Critical section - print stuff here.
        >>> states.append(bool(printer_lock.locked()))
        >>> states
        [True, False]

        >>> states = []
        >>> with printer_lock:
        ...     states.append(bool(printer_lock.locked()))
        ...     # Critical section - print stuff here.
        >>> states.append(bool(printer_lock.locked()))
        >>> states
        [True, False]
    '''

    KEY_PREFIX = 'redlock'
    AUTO_RELEASE_TIME = 10 * 1000
    CLOCK_DRIFT_FACTOR = 0.01
    RETRY_DELAY = 200
    NUM_EXTENSIONS = 3
    NUM_RANDOM_BYTES = 20

    def __init__(self, *, key, masters=frozenset(),
                 auto_release_time=AUTO_RELEASE_TIME,
                 num_extensions=NUM_EXTENSIONS,
                 num_random_bytes=NUM_RANDOM_BYTES):
        super().__init__(key=key, masters=masters)
        self.auto_release_time = auto_release_time
        self.num_extensions = num_extensions
        self.num_random_bytes = num_random_bytes

        self._value = b''
        self._extension_num = 0
        self._acquired_script = self._register_acquired_script()
        self._extend_script = self._register_extend_script()
        self._release_script = self._register_release_script()

    def _register_acquired_script(self):
        master = next(iter(self.masters))
        acquired_script = master.register_script('''
            if redis.call('get', KEYS[1]) == ARGV[1] then
                local pttl = redis.call('pttl', KEYS[1])
                return (pttl > 0) and pttl or 0
            else
                return 0
            end
        ''')
        return acquired_script

    def _register_extend_script(self):
        master = next(iter(self.masters))
        extend_script = master.register_script('''
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('pexpire', KEYS[1], ARGV[2])
            else
                return 0
            end
        ''')
        return extend_script

    def _register_release_script(self):
        master = next(iter(self.masters))
        release_script = master.register_script('''
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
        ''')
        return release_script

    def _acquire_master(self, master):
        acquired = master.set(
            self.key,
            self._value,
            px=self.auto_release_time,
            nx=True,
        )
        return bool(acquired)

    def _acquired_master(self, master):
        if self._value:
            ttl = self._acquired_script(
                keys=(self.key,),
                args=(self._value,),
                client=master,
            )
        else:
            ttl = 0
        return ttl

    def _extend_master(self, master):
        extended = self._extend_script(
            keys=(self.key,),
            args=(self._value, self.auto_release_time),
            client=master,
        )
        return bool(extended)

    def _release_master(self, master):
        released = self._release_script(
            keys=(self.key,),
            args=(self._value,),
            client=master,
        )
        return bool(released)

    def _drift(self):
        return self.auto_release_time * self.CLOCK_DRIFT_FACTOR + 2

    def _acquire_masters(self):
        self._value = os.urandom(self.num_random_bytes)
        self._extension_num = 0
        futures, num_masters_acquired = set(), 0
        with ContextTimer() as timer, \
             concurrent.futures.ThreadPoolExecutor() as executor:
            for master in self.masters:
                futures.add(executor.submit(self._acquire_master, master))
            for future in concurrent.futures.as_completed(futures):
                with contextlib.suppress(TimeoutError, ConnectionError):
                    num_masters_acquired += future.result()
            quorum = num_masters_acquired >= len(self.masters) // 2 + 1
            elapsed = timer.elapsed() - self._drift()
            validity_time = self.auto_release_time - elapsed
        if quorum and max(validity_time, 0):
            return True
        else:
            with contextlib.suppress(ReleaseUnlockedLock):
                self.release()
            return False

    def acquire(self, *, blocking=True, timeout=-1):
        '''Lock the lock.

        If blocking is True and timeout is -1, then wait for as long as
        necessary to acquire the lock.  Return True.

            >>> printer_lock_1 = Redlock(key='printer')
            >>> printer_lock_1.acquire()
            True
            >>> timer = ContextTimer()
            >>> timer.start()
            >>> printer_lock_2 = Redlock(key='printer')
            >>> printer_lock_2.acquire()
            True
            >>> 10 * 1000 < timer.elapsed() < 11 * 1000
            True
            >>> printer_lock_2.release()

        If blocking is True and timeout is not -1, then wait for up to timeout
        seconds to acquire the lock.  Return True if the lock was acquired;
        False if it wasn't.

            >>> printer_lock_1.acquire()
            True
            >>> printer_lock_2.acquire(timeout=15)
            True
            >>> printer_lock_2.release()

            >>> printer_lock_1.acquire()
            True
            >>> printer_lock_2.acquire(timeout=1)
            False
            >>> printer_lock_1.release()

        If blocking is False and timeout is -1, then try just once right now to
        acquire the lock.  Return True if the lock was acquired; False if it
        wasn't.

            >>> printer_lock_1.acquire()
            True
            >>> printer_lock_2.acquire(blocking=False)
            False
            >>> printer_lock_1.release()
        '''
        if blocking:
            with ContextTimer() as timer:
                while timeout == -1 or timer.elapsed() / 1000 < timeout:
                    if self._acquire_masters():
                        return True
                    else:
                        time.sleep(random.uniform(0, self.RETRY_DELAY/1000))
            return False
        elif timeout == -1:
            return self._acquire_masters()
        else:
            raise ValueError("can't specify a timeout for a non-blocking call")

    def locked(self):
        '''How much longer we'll hold the lock (unless we extend or release it).

        If we don't currently hold the lock, then this method returns 0.

            >>> printer_lock_1 = Redlock(key='printer')
            >>> printer_lock_1.locked()
            0

            >>> printer_lock_2 = Redlock(key='printer')
            >>> printer_lock_2.acquire()
            True
            >>> printer_lock_1.locked()
            0
            >>> printer_lock_2.release()

        If we do currently hold the lock, then this method returns the current
        lease's Time To Live (TTL) in ms.

            >>> printer_lock_1.acquire()
            True
            >>> 9 * 1000 < printer_lock_1.locked() < 10 * 1000
            True
            >>> printer_lock_1.release()

        '''
        futures, num_masters_acquired, ttls = set(), 0, []
        with ContextTimer() as timer, \
             concurrent.futures.ThreadPoolExecutor() as executor:
            for master in self.masters:
                futures.add(executor.submit(self._acquired_master, master))
            for future in concurrent.futures.as_completed(futures):
                with contextlib.suppress(TimeoutError, ConnectionError):
                    ttl = future.result()
                    num_masters_acquired += ttl > 0
                    ttls.append(ttl)
            quorum = num_masters_acquired >= len(self.masters) // 2 + 1
            if quorum:
                ttls = sorted(ttls, reverse=True)
                validity_time = ttls[len(self.masters) // 2]
                validity_time -= timer.elapsed() + self._drift()
                return max(validity_time, 0)
            else:
                return 0

    def extend(self):
        '''Extend our hold on the lock (if we currently hold it).

        Usage:

            >>> printer_lock = Redlock(key='printer')
            >>> printer_lock.acquire()
            True
            >>> 9 * 1000 < printer_lock.locked() < 10 * 1000
            True
            >>> time.sleep(1)
            >>> 8 * 1000 < printer_lock.locked() < 9 * 1000
            True
            >>> printer_lock.extend()
            True
            >>> 9 * 1000 < printer_lock.locked() < 10 * 1000
            True
            >>> printer_lock.release()
        '''
        if self._extension_num >= self.num_extensions:
            raise TooManyExtensions(self.masters, self.key)
        else:
            futures, num_masters_extended = set(), 0
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for master in self.masters:
                    futures.add(executor.submit(self._extend_master, master))
                for future in concurrent.futures.as_completed(futures):
                    with contextlib.suppress(TimeoutError, ConnectionError):
                        num_masters_extended += future.result()
            quorum = num_masters_extended >= len(self.masters) // 2 + 1
            self._extension_num += quorum
            return quorum

    def release(self):
        '''Unlock the lock.

        Usage:

            >>> printer_lock = Redlock(key='printer')
            >>> bool(printer_lock.locked())
            False
            >>> printer_lock.acquire()
            True
            >>> bool(printer_lock.locked())
            True
            >>> printer_lock.release()
            >>> bool(printer_lock.locked())
            False
        '''
        futures, num_masters_released = set(), 0
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for master in self.masters:
                futures.add(executor.submit(self._release_master, master))
            for future in concurrent.futures.as_completed(futures):
                with contextlib.suppress(TimeoutError, ConnectionError):
                    num_masters_released += future.result()
        quorum = num_masters_released >= len(self.masters) // 2 + 1
        if not quorum:
            raise ReleaseUnlockedLock(self.masters, self.key)

    def __enter__(self):
        '''You can use a Redlock as a context manager.

        Usage:

            >>> states = []
            >>> with Redlock(key='printer') as printer_lock:
            ...     states.append(bool(printer_lock.locked()))
            ...     # Critical section - print stuff here.
            >>> states.append(bool(printer_lock.locked()))
            >>> states
            [True, False]

            >>> states = []
            >>> with printer_lock:
            ...     states.append(bool(printer_lock.locked()))
            ...     # Critical section - print stuff here.
            >>> states.append(bool(printer_lock.locked()))
            >>> states
            [True, False]
        '''
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        '''You can use a Redlock as a context manager.

        Usage:

            >>> states = []
            >>> with Redlock(key='printer') as printer_lock:
            ...     states.append(bool(printer_lock.locked()))
            ...     # Critical section - print stuff here.
            >>> states.append(bool(printer_lock.locked()))
            >>> states
            [True, False]

            >>> states = []
            >>> with printer_lock:
            ...     states.append(bool(printer_lock.locked()))
            ...     # Critical section - print stuff here.
            >>> states.append(bool(printer_lock.locked()))
            >>> states
            [True, False]
        '''
        self.release()

    def __repr__(self):
        return '<{} key={} value={} timeout={}>'.format(
            self.__class__.__name__,
            self.key,
            self._value,
            self.locked(),
        )


if __name__ == '__main__':  # pragma: no cover
    # Run the doctests in this module with:
    #   $ source venv/bin/activate
    #   $ python3 -m pottery.redlock
    #   $ deactivate
    with contextlib.suppress(ImportError):
        from tests.base import run_doctests
        run_doctests()
