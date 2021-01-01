# --------------------------------------------------------------------------- #
#   redlock.py                                                                #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
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
import functools
import logging
import os
import random
import time
from types import TracebackType
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Iterable
from typing import Optional
from typing import Type
from typing import cast

from redis import Redis
from redis import RedisError
from redis.client import Script
from typing_extensions import Final

from .annotations import F
from .base import Primitive
from .exceptions import ExtendUnlockedLock
from .exceptions import ReleaseUnlockedLock
from .exceptions import TooManyExtensions
from .executor import BailOutExecutor
from .timer import ContextTimer


AUTO_RELEASE_TIME: Final[int] = 10 * 1000


_logger: Final[logging.Logger] = logging.getLogger('pottery')


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

    KEY_PREFIX: ClassVar[str] = 'redlock'
    CLOCK_DRIFT_FACTOR: ClassVar[float] = 0.01
    RETRY_DELAY: ClassVar[int] = 200
    NUM_EXTENSIONS: ClassVar[int] = 3
    NUM_RANDOM_BYTES: ClassVar[int] = 20

    _acquired_script: ClassVar[Optional[Script]] = None
    _extend_script: ClassVar[Optional[Script]] = None
    _release_script: ClassVar[Optional[Script]] = None

    def __init__(self,
                 *,
                 key: str,
                 masters: Iterable[Redis] = frozenset(),
                 auto_release_time: int = AUTO_RELEASE_TIME,
                 num_extensions: int = NUM_EXTENSIONS,
                 num_random_bytes: int = NUM_RANDOM_BYTES,
                 ) -> None:
        super().__init__(key=key, masters=masters)
        self.__register_acquired_script()
        self.__register_extend_script()
        self.__register_release_script()

        self.auto_release_time = auto_release_time
        self.num_extensions = num_extensions
        self.num_random_bytes = num_random_bytes

        self._value = b''
        self._extension_num = 0

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    def __register_acquired_script(self) -> None:
        if self._acquired_script is None:
            _logger.info(
                'Registering %s._acquired_script',
                self.__class__.__name__,
            )
            master = next(iter(self.masters))
            self.__class__._acquired_script = master.register_script('''
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    local pttl = redis.call('pttl', KEYS[1])
                    return (pttl > 0) and pttl or 0
                else
                    return 0
                end
            ''')

    def __register_extend_script(self) -> None:
        if self._extend_script is None:
            _logger.info(
                'Registering %s._extend_script',
                self.__class__.__name__,
            )
            master = next(iter(self.masters))
            self.__class__._extend_script = master.register_script('''
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    return redis.call('pexpire', KEYS[1], ARGV[2])
                else
                    return 0
                end
            ''')

    def __register_release_script(self) -> None:
        if self._release_script is None:
            _logger.info(
                'Registering %s._release_script',
                self.__class__.__name__,
            )
            master = next(iter(self.masters))
            self.__class__._release_script = master.register_script('''
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    return redis.call('del', KEYS[1])
                else
                    return 0
                end
            ''')

    def __acquire_master(self, master: Redis) -> bool:
        acquired = master.set(
            self.key,
            self._value,
            px=self.auto_release_time,
            nx=True,
        )
        return bool(acquired)

    def __acquired_master(self, master: Redis) -> int:
        if self._value:
            ttl: int = cast(Script, self._acquired_script)(
                keys=(self.key,),
                args=(self._value,),
                client=master,
            )
        else:
            ttl = 0
        return ttl

    def __extend_master(self, master: Redis) -> bool:
        extended = cast(Script, self._extend_script)(
            keys=(self.key,),
            args=(self._value, self.auto_release_time),
            client=master,
        )
        return bool(extended)

    def __release_master(self, master: Redis) -> bool:
        released = cast(Script, self._release_script)(
            keys=(self.key,),
            args=(self._value,),
            client=master,
        )
        return bool(released)

    def __drift(self) -> float:
        return self.auto_release_time * self.CLOCK_DRIFT_FACTOR + 2

    def __acquire_masters(self) -> bool:
        self._value = os.urandom(self.num_random_bytes)
        self._extension_num = 0
        quorum, validity_time = False, 0.0

        with ContextTimer() as timer, BailOutExecutor() as executor:
            futures = set()
            for master in self.masters:
                future = executor.submit(self.__acquire_master, master)
                futures.add(future)

            num_masters_acquired = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    num_masters_acquired += future.result()
                except RedisError as error:  # pragma: no cover
                    _logger.exception(
                        '%s.__acquire_masters() caught an %s',
                        self.__class__.__name__,
                        error.__class__.__name__,
                    )
                else:
                    quorum = num_masters_acquired >= len(self.masters) // 2 + 1
                    if quorum:
                        elapsed = timer.elapsed() - self.__drift()
                        validity_time = self.auto_release_time - elapsed
                        break

        if quorum and max(validity_time, 0):
            return True
        else:
            with contextlib.suppress(ReleaseUnlockedLock):
                self.__release()
            return False

    def acquire(self, *, blocking: bool = True, timeout: int = -1) -> bool:
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
                    if self.__acquire_masters():
                        return True
                    else:
                        time.sleep(random.uniform(0, self.RETRY_DELAY/1000))
            return False
        elif timeout == -1:
            return self.__acquire_masters()
        else:
            raise ValueError("can't specify a timeout for a non-blocking call")

    __acquire = acquire

    def locked(self) -> int:
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
        with ContextTimer() as timer, \
             concurrent.futures.ThreadPoolExecutor() as executor:
            futures = set()
            for master in self.masters:
                future = executor.submit(self.__acquired_master, master)
                futures.add(future)

            ttls = []
            for future in concurrent.futures.as_completed(futures):
                try:
                    ttls.append(future.result())
                except RedisError as error:  # pragma: no cover
                    _logger.exception(
                        '%s.locked() caught an %s',
                        self.__class__.__name__,
                        error.__class__.__name__,
                    )

            num_masters_acquired = sum(1 for ttl in ttls if ttl > 0)
            quorum = num_masters_acquired >= len(self.masters) // 2 + 1
            if quorum:
                ttls = sorted(ttls, reverse=True)
                validity_time = ttls[len(self.masters) // 2]
                validity_time -= round(timer.elapsed() + self.__drift())
                return max(validity_time, 0)
            else:
                return 0

    __locked = locked

    def extend(self) -> None:
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
            >>> 9 * 1000 < printer_lock.locked() < 10 * 1000
            True
            >>> printer_lock.release()
        '''
        if self._extension_num >= self.num_extensions:
            raise TooManyExtensions(self.masters, self.key)
        else:
            quorum = False

            with BailOutExecutor() as executor:
                futures = set()
                for master in self.masters:
                    future = executor.submit(self.__extend_master, master)
                    futures.add(future)

                num_masters_extended = 0
                for future in concurrent.futures.as_completed(futures):
                    try:
                        num_masters_extended += future.result()
                    except RedisError as error:  # pragma: no cover
                        _logger.exception(
                            '%s.extend() caught an %s',
                            self.__class__.__name__,
                            error.__class__.__name__,
                        )
                    else:
                        quorum = num_masters_extended >= len(self.masters) // 2 + 1
                        if quorum:
                            break

            self._extension_num += quorum
            if not quorum:
                raise ExtendUnlockedLock(self.masters, self.key)

    def release(self) -> None:
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
        quorum = False

        with BailOutExecutor() as executor:
            futures = set()
            for master in self.masters:
                future = executor.submit(self.__release_master, master)
                futures.add(future)

            num_masters_released = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    num_masters_released += future.result()
                except RedisError as error:  # pragma: no cover
                    _logger.exception(
                        '%s.release() caught an %s',
                        self.__class__.__name__,
                        error.__class__.__name__,
                    )
                else:
                    quorum = num_masters_released >= len(self.masters) // 2 + 1
                    if quorum:
                        break

        if not quorum:
            raise ReleaseUnlockedLock(self.masters, self.key)

    __release = release

    def __enter__(self) -> 'Redlock':
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
        self.__acquire()
        return self

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType],
                 ) -> None:
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
        self.__release()

    def __repr__(self) -> str:
        return (
            f'<{self.__class__.__name__} key={self.key} '
            f'value={str(self._value)} timeout={self.__locked()}>'
        )


def synchronize(*,
                key: str,
                masters: Iterable[Redis] = frozenset(),
                auto_release_time: int = AUTO_RELEASE_TIME,
                ) -> Callable[[F], F]:
    '''Decorator to synchronize a function's execution across threads.

    synchronize() is a decorator that allows only one thread to execute a
    function at a time.  Under the hood, synchronize() uses a Redlock.  See
    help(Redlock) for more details.

    Usage:

        >>> @synchronize(key='synchronized-func', auto_release_time=1500)
        ... def func():
        ...     # Only one thread can execute this function at a time.
        ...     return True
        ...
        >>> func()
        True
    '''
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            redlock = Redlock(
                key=key,
                masters=masters,
                auto_release_time=auto_release_time,
            )
            with ContextTimer() as timer, redlock:
                return_value = func(*args, **kwargs)

            _logger.info(
                '%s() held %s for %d ms',
                func.__qualname__,
                redlock.key,
                timer.elapsed(),
            )
            return return_value
        return cast(F, wrapper)
    return decorator


if __name__ == '__main__':  # pragma: no cover
    # Run the doctests in this module with:
    #   $ source venv/bin/activate
    #   $ python3 -m pottery.redlock
    #   $ deactivate
    with contextlib.suppress(ImportError):
        from tests.base import run_doctests  # type: ignore
        run_doctests()
