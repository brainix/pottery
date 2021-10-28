# --------------------------------------------------------------------------- #
#   redlock.py                                                                #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#                                                                             #
#   Licensed under the Apache License, Version 2.0 (the "License");           #
#   you may not use this file except in compliance with the License.          #
#   You may obtain a copy of the License at:                                  #
#       http://www.apache.org/licenses/LICENSE-2.0                            #
#                                                                             #
#   Unless required by applicable law or agreed to in writing, software       #
#   distributed under the License is distributed on an "AS IS" BASIS,         #
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  #
#   See the License for the specific language governing permissions and       #
#   limitations under the License.                                            #
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
import math
import random
import time
import uuid
from types import TracebackType
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Iterable
from typing import Optional
from typing import Tuple
from typing import Type
from typing import cast
from typing import overload

from redis import Redis
from redis import RedisError
from redis.client import Script
from typing_extensions import Final
from typing_extensions import Literal

from .annotations import F
from .base import Primitive
from .exceptions import ExtendUnlockedLock
from .exceptions import QuorumNotAchieved
from .exceptions import ReleaseUnlockedLock
from .exceptions import TooManyExtensions
from .executor import BailOutExecutor
from .timer import ContextTimer


AUTO_RELEASE_TIME: Final[int] = 10 * 1000


_logger: Final[logging.Logger] = logging.getLogger('pottery')


class _Scripts:
    '''Parent class to define/register Lua scripts for Redis.

    Note that we only have to register these Lua scripts once -- so we do it on
    the first instantiation of Redlock.
    '''

    __slots__: Tuple[str, ...] = tuple()

    _acquired_script: ClassVar[Optional[Script]] = None
    _extend_script: ClassVar[Optional[Script]] = None
    _release_script: ClassVar[Optional[Script]] = None

    def __init__(self,
                 *,
                 key: str,
                 masters: Iterable[Redis] = frozenset(),
                 raise_on_redis_errors: bool = False,
                 ) -> None:
        super().__init__(  # type: ignore
            key=key,
            masters=masters,
            raise_on_redis_errors=raise_on_redis_errors,
        )
        self.__register_acquired_script()
        self.__register_extend_script()
        self.__register_release_script()

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    def __register_acquired_script(self) -> None:
        if self._acquired_script is None:
            class_name = self.__class__.__qualname__
            _logger.info('Registering %s._acquired_script', class_name)
            master = next(iter(self.masters))  # type: ignore
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
            class_name = self.__class__.__qualname__
            _logger.info('Registering %s._extend_script', class_name)
            master = next(iter(self.masters))  # type: ignore
            self.__class__._extend_script = master.register_script('''
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    return redis.call('pexpire', KEYS[1], ARGV[2])
                else
                    return 0
                end
            ''')

    def __register_release_script(self) -> None:
        if self._release_script is None:
            class_name = self.__class__.__qualname__
            _logger.info('Registering %s._release_script', class_name)
            master = next(iter(self.masters))  # type: ignore
            self.__class__._release_script = master.register_script('''
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    return redis.call('del', KEYS[1])
                else
                    return 0
                end
            ''')


class Redlock(_Scripts, Primitive):
    '''Distributed Redis-powered lock.

    This algorithm safely and reliably provides a mutually-exclusive locking
    primitive to protect a resource shared across threads, processes, and even
    machines, without a single point of failure.

    Rationale and algorithm description:
        http://redis.io/topics/distlock

    Usage:

        >>> from redis import Redis
        >>> redis = Redis(socket_timeout=1)
        >>> printer_lock = Redlock(key='printer', masters={redis})
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

        >>> printer_lock = Redlock(key='printer', masters={redis}, auto_release_time=15*1000)
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
        >>> with Redlock(key='printer', masters={redis}) as printer_lock:
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

    __slots__ = (
        'auto_release_time',
        'num_extensions',
        'context_manager_blocking',
        'context_manager_timeout',
        '_uuid',
        '_extension_num',
    )

    KEY_PREFIX: ClassVar[str] = 'redlock'
    CLOCK_DRIFT_FACTOR: ClassVar[float] = 0.01
    RETRY_DELAY: ClassVar[int] = 200
    NUM_EXTENSIONS: ClassVar[int] = 3

    def __init__(self,
                 *,
                 key: str,
                 masters: Iterable[Redis] = frozenset(),
                 raise_on_redis_errors: bool = False,
                 auto_release_time: int = AUTO_RELEASE_TIME,
                 num_extensions: int = NUM_EXTENSIONS,
                 context_manager_blocking: bool = True,
                 context_manager_timeout: float = -1,
                 ) -> None:
        if not context_manager_blocking and context_manager_timeout != -1:
            raise ValueError("can't specify a timeout for a non-blocking call")

        super().__init__(
            key=key,
            masters=masters,
            raise_on_redis_errors=raise_on_redis_errors,
        )
        self.auto_release_time = auto_release_time
        self.num_extensions = num_extensions
        self.context_manager_blocking = context_manager_blocking
        self.context_manager_timeout = context_manager_timeout
        self._uuid = ''
        self._extension_num = 0

    def __acquire_master(self, master: Redis) -> bool:
        acquired = master.set(
            self.key,
            self._uuid,
            px=self.auto_release_time,
            nx=True,
        )
        return bool(acquired)

    def __acquired_master(self, master: Redis) -> int:
        if self._uuid:
            ttl: int = cast(Script, self._acquired_script)(
                keys=(self.key,),
                args=(self._uuid,),
                client=master,
            )
        else:
            ttl = 0
        return ttl

    def __extend_master(self, master: Redis) -> bool:
        extended = cast(Script, self._extend_script)(
            keys=(self.key,),
            args=(self._uuid, self.auto_release_time),
            client=master,
        )
        return bool(extended)

    def __release_master(self, master: Redis) -> bool:
        released = cast(Script, self._release_script)(
            keys=(self.key,),
            args=(self._uuid,),
            client=master,
        )
        return bool(released)

    def __drift(self) -> float:
        return self.auto_release_time * self.CLOCK_DRIFT_FACTOR + 2

    def __acquire_masters(self,
                          *,
                          raise_on_redis_errors: Optional[bool] = None,
                          ) -> bool:
        self._uuid = str(uuid.uuid4())
        self._extension_num = 0

        with ContextTimer() as timer, BailOutExecutor() as executor:
            futures = set()
            for master in self.masters:
                future = executor.submit(self.__acquire_master, master)
                futures.add(future)

            num_masters_acquired, redis_errors = 0, []
            for future in concurrent.futures.as_completed(futures):
                try:
                    num_masters_acquired += future.result()
                except RedisError as error:
                    redis_errors.append(error)
                    _logger.exception(
                        '%s.__acquire_masters() caught %s',
                        self.__class__.__name__,
                        error.__class__.__name__,
                    )
                else:
                    if num_masters_acquired > len(self.masters) // 2:
                        validity_time = self.auto_release_time
                        validity_time -= round(self.__drift())
                        validity_time -= timer.elapsed()
                        if validity_time > 0:  # pragma: no cover
                            return True

        with contextlib.suppress(ReleaseUnlockedLock):
            self.__release(raise_on_redis_errors=False)
        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        return False

    def acquire(self,
                *,
                blocking: bool = True,
                timeout: float = -1,
                raise_on_redis_errors: Optional[bool] = None,
                ) -> bool:
        '''Lock the lock.

        If blocking is True and timeout is -1, then wait for as long as
        necessary to acquire the lock.  Return True.

            >>> from redis import Redis
            >>> redis = Redis(socket_timeout=1)
            >>> printer_lock_1 = Redlock(key='printer', masters={redis})
            >>> printer_lock_1.acquire()
            True
            >>> timer = ContextTimer()
            >>> timer.start()
            >>> printer_lock_2 = Redlock(key='printer', masters={redis})
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
        acquire_masters = functools.partial(
            self.__acquire_masters,
            raise_on_redis_errors=raise_on_redis_errors,
        )

        def log_time_enqueued(timer: ContextTimer, acquired: bool) -> None:
            key_suffix = self.key.split(':', maxsplit=1)[1]
            time_enqueued = math.ceil(timer.elapsed())
            _logger.info(
                'source=pottery sample#redlock.enqueued.%s=%dms sample#redlock.acquired.%s=%d',
                key_suffix,
                time_enqueued,
                key_suffix,
                acquired,
            )

        if blocking:
            enqueued = False
            with ContextTimer() as timer:
                while timeout == -1 or timer.elapsed() / 1000 < timeout:
                    if acquire_masters():
                        if enqueued:
                            log_time_enqueued(timer, True)
                        return True
                    enqueued = True
                    time.sleep(random.uniform(0, self.RETRY_DELAY/1000))
            if enqueued:
                log_time_enqueued(timer, False)
            return False

        if timeout == -1:
            return acquire_masters()

        raise ValueError("can't specify a timeout for a non-blocking call")

    __acquire = acquire

    def locked(self, *, raise_on_redis_errors: Optional[bool] = None) -> int:
        '''How much longer we'll hold the lock (unless we extend or release it).

        If we don't currently hold the lock, then this method returns 0.

            >>> from redis import Redis
            >>> redis = Redis(socket_timeout=1)
            >>> printer_lock_1 = Redlock(key='printer', masters={redis})
            >>> printer_lock_1.locked()
            0

            >>> printer_lock_2 = Redlock(key='printer', masters={redis})
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
        with ContextTimer() as timer, BailOutExecutor() as executor:
            futures = set()
            for master in self.masters:
                future = executor.submit(self.__acquired_master, master)
                futures.add(future)

            ttls, redis_errors = [], []
            for future in concurrent.futures.as_completed(futures):
                try:
                    ttl = future.result()
                except RedisError as error:
                    redis_errors.append(error)
                    _logger.exception(
                        '%s.locked() caught %s',
                        self.__class__.__name__,
                        error.__class__.__name__,
                    )
                else:
                    if ttl:
                        ttls.append(ttl)
                        if len(ttls) > len(self.masters) // 2:  # pragma: no cover
                            validity_time = min(ttls)
                            validity_time -= round(self.__drift())
                            validity_time -= timer.elapsed()
                            return max(validity_time, 0)

        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        return 0

    __locked = locked

    def extend(self, *, raise_on_redis_errors: Optional[bool] = None) -> None:
        '''Extend our hold on the lock (if we currently hold it).

        Usage:

            >>> from redis import Redis
            >>> redis = Redis(socket_timeout=1)
            >>> printer_lock = Redlock(key='printer', masters={redis})
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
            raise TooManyExtensions(self.key, self.masters)

        with BailOutExecutor() as executor:
            futures = set()
            for master in self.masters:
                future = executor.submit(self.__extend_master, master)
                futures.add(future)

            num_masters_extended, redis_errors = 0, []
            for future in concurrent.futures.as_completed(futures):
                try:
                    num_masters_extended += future.result()
                except RedisError as error:
                    redis_errors.append(error)
                    _logger.exception(
                        '%s.extend() caught %s',
                        self.__class__.__name__,
                        error.__class__.__name__,
                    )
                else:
                    if num_masters_extended > len(self.masters) // 2:
                        self._extension_num += 1
                        return

        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        raise ExtendUnlockedLock(
            self.key,
            self.masters,
            redis_errors=redis_errors,
        )

    def release(self, *, raise_on_redis_errors: Optional[bool] = None) -> None:
        '''Unlock the lock.

        Usage:

            >>> from redis import Redis
            >>> redis = Redis(socket_timeout=1)
            >>> printer_lock = Redlock(key='printer', masters={redis})
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
        with BailOutExecutor() as executor:
            futures = set()
            for master in self.masters:
                future = executor.submit(self.__release_master, master)
                futures.add(future)

            num_masters_released, redis_errors = 0, []
            for future in concurrent.futures.as_completed(futures):
                try:
                    num_masters_released += future.result()
                except RedisError as error:
                    redis_errors.append(error)
                    _logger.exception(
                        '%s.release() caught %s',
                        self.__class__.__name__,
                        error.__class__.__name__,
                    )
                else:
                    if num_masters_released > len(self.masters) // 2:
                        return

        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        raise ReleaseUnlockedLock(
            self.key,
            self.masters,
            redis_errors=redis_errors,
        )

    __release = release

    def __enter__(self) -> 'Redlock':
        '''You can use a Redlock as a context manager.

        Usage:

            >>> states = []
            >>> from redis import Redis
            >>> redis = Redis(socket_timeout=1)
            >>> with Redlock(key='printer', masters={redis}) as printer_lock:
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
        acquired = self.__acquire(
            blocking=self.context_manager_blocking,
            timeout=self.context_manager_timeout,
        )
        if acquired:
            return self
        raise QuorumNotAchieved(self.key, self.masters)

    @overload
    def __exit__(self,
                 exc_type: None,
                 exc_value: None,
                 exc_traceback: None,
                 ) -> Literal[False]:
        raise NotImplementedError

    @overload
    def __exit__(self,
                 exc_type: Type[BaseException],
                 exc_value: BaseException,
                 exc_traceback: TracebackType,
                 ) -> Literal[False]:
        raise NotImplementedError

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType],
                 ) -> Literal[False]:
        '''You can use a Redlock as a context manager.

        Usage:

            >>> states = []
            >>> from redis import Redis
            >>> redis = Redis(socket_timeout=1)
            >>> with Redlock(key='printer', masters={redis}) as printer_lock:
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
        return False

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} key={self.key}>'


def synchronize(*,
                key: str,
                masters: Iterable[Redis] = frozenset(),
                raise_on_redis_errors: bool = False,
                auto_release_time: int = AUTO_RELEASE_TIME,
                blocking: bool = True,
                timeout: float = -1,
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
    RedlockFactory = functools.partial(
        Redlock,
        key=key,
        masters=masters,
        raise_on_redis_errors=raise_on_redis_errors,
        auto_release_time=auto_release_time,
        context_manager_blocking=blocking,
        context_manager_timeout=timeout,
    )

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            redlock = RedlockFactory()
            waiting_timer, holding_timer = ContextTimer(), ContextTimer()

            try:
                waiting_timer.start()
                with redlock:
                    waiting_timer.stop()
                    holding_timer.start()
                    return_value = func(*args, **kwargs)
                holding_timer.stop()
            finally:
                _log_synchronize(func, redlock, waiting_timer, holding_timer)

            return return_value
        return cast(F, wrapper)
    return decorator


def _log_synchronize(func: F,
                     redlock: Redlock,
                     waiting_timer: ContextTimer,
                     holding_timer: ContextTimer,
                     ) -> None:
    try:
        _logger.info(
            '%s() waited for %s for %d ms; held for %d ms',
            func.__qualname__,
            redlock.key,
            waiting_timer.elapsed(),
            holding_timer.elapsed(),
        )
    except RuntimeError:  # pragma: no cover
        # holding_timer.elapsed() threw a RuntimeError, which means that
        # holding_timer never started, which means that we never acquired the
        # lock / entered the critical section.
        _logger.info(
            '%s() waited for %s for %d ms; never acquired lock',
            func.__qualname__,
            redlock.key,
            waiting_timer.elapsed(),
        )


if __name__ == '__main__':
    # Run the doctests in this module with:
    #   $ source venv/bin/activate
    #   $ python3 -m pottery.redlock
    #   $ deactivate
    with contextlib.suppress(ImportError):
        from tests.base import run_doctests
        run_doctests()
