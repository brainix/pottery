# --------------------------------------------------------------------------- #
#   aioredlock.py                                                             #
#                                                                             #
#   Copyright © 2015-2022, Rajiv Bakulesh Shah, original author.              #
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
'''Asynchronous distributed Redis-powered lock.

This algorithm safely and reliably provides a mutually-exclusive locking
primitive to protect a resource shared across coroutines, threads, processes,
and even machines, without a single point of failure.

Rationale and algorithm description:
    http://redis.io/topics/distlock

Reference implementations:
    https://github.com/antirez/redlock-rb
    https://github.com/SPSCommerce/redlock-py

Lua scripting:
    https://github.com/andymccurdy/redis-py#lua-scripting
'''


# TODO: Remove the following import after deferred evaluation of annotations
# because the default.
#   1. https://docs.python.org/3/whatsnew/3.7.html#whatsnew37-pep563
#   2. https://www.python.org/dev/peps/pep-0563/
#   3. https://www.python.org/dev/peps/pep-0649/
from __future__ import annotations

import asyncio
import contextlib
import functools
import math
import random
import uuid
from types import TracebackType
from typing import ClassVar
from typing import Iterable
from typing import Type

from redis import RedisError
from redis.asyncio import Redis as AIORedis  # type: ignore

from .base import AIOPrimitive
from .base import logger
from .exceptions import ExtendUnlockedLock
from .exceptions import QuorumNotAchieved
from .exceptions import ReleaseUnlockedLock
from .exceptions import TooManyExtensions
from .redlock import Redlock
from .redlock import Scripts
from .timer import ContextTimer


class AIORedlock(Scripts, AIOPrimitive):
    '''Asynchronous distributed Redis-powered lock.

    This algorithm safely and reliably provides a mutually-exclusive locking
    primitive to protect a resource shared across coroutines, threads,
    processes, and even machines, without a single point of failure.

    Rationale and algorithm description:
        http://redis.io/topics/distlock

    Usage:

        >>> import asyncio
        >>> from redis.asyncio import Redis as AIORedis
        >>> async def main():
        ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
        ...     shower = AIORedlock(key='shower', masters={aioredis})
        ...     if await shower.acquire():
        ...         # Critical section - no other coroutine can enter while we hold the lock.
        ...         print(f"shower is {'occupied' if await shower.locked() else 'available'}")
        ...         await shower.release()
        ...     print(f"shower is {'occupied' if await shower.locked() else 'available'}")
        >>> asyncio.run(main(), debug=True)
        shower is occupied
        shower is available

    AIORedlocks time out (by default, after 10 seconds).  You should take care
    to ensure that your critical section completes well within the timeout.  The
    reasons that AIORedlocks time out are to preserve "liveness"
    (http://redis.io/topics/distlock#liveness-arguments) and to avoid deadlocks
    (in the event that a process dies inside a critical section before it
    releases its lock).

        >>> async def main():
        ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
        ...     shower = AIORedlock(key='shower', masters={aioredis}, auto_release_time=.2)
        ...     if await shower.acquire():
        ...         # Critical section - no other coroutine can enter while we hold the lock.
        ...         await asyncio.sleep(shower.auto_release_time)
        ...     print(f"shower is {'occupied' if await shower.locked() else 'available'}")
        >>> asyncio.run(main(), debug=True)
        shower is available

    If 10 seconds isn't enough to complete executing your critical section,
    then you can specify your own timeout:

        >>> async def main():
        ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
        ...     shower = AIORedlock(key='shower', masters={aioredis}, auto_release_time=.2)
        ...     if await shower.acquire():
        ...         # Critical section - no other coroutine can enter while we hold the lock.
        ...         await asyncio.sleep(shower.auto_release_time / 2)
        ...     print(f"shower is {'occupied' if await shower.locked() else 'available'}")
        ...     await asyncio.sleep(shower.auto_release_time / 2)
        ...     print(f"shower is {'occupied' if await shower.locked() else 'available'}")
        >>> asyncio.run(main(), debug=True)
        shower is occupied
        shower is available

    You can use an AIORedlock as a context manager:

        >>> async def main():
        ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
        ...     shower = AIORedlock(key='shower', masters={aioredis})
        ...     async with shower:
        ...         print(f"shower is {'occupied' if await shower.locked() else 'available'}")
        ...         # Critical section - no other coroutine can enter while we hold the lock.
        ...     print(f"shower is {'occupied' if await shower.locked() else 'available'}")
        >>> asyncio.run(main(), debug=True)
        shower is occupied
        shower is available
    '''

    __slots__ = (
        'auto_release_time',
        'num_extensions',
        'context_manager_blocking',
        'context_manager_timeout',
        '_uuid',
        '_extension_num',
    )

    _KEY_PREFIX: ClassVar[str] = Redlock._KEY_PREFIX
    _AUTO_RELEASE_TIME: ClassVar[float] = Redlock._AUTO_RELEASE_TIME
    _CLOCK_DRIFT_FACTOR: ClassVar[float] = Redlock._CLOCK_DRIFT_FACTOR
    _RETRY_DELAY: ClassVar[float] = Redlock._RETRY_DELAY
    _NUM_EXTENSIONS: ClassVar[int] = Redlock._NUM_EXTENSIONS

    def __init__(self,  # type: ignore
                 *,
                 key: str,
                 masters: Iterable[AIORedis],
                 raise_on_redis_errors: bool = False,
                 auto_release_time: float = _AUTO_RELEASE_TIME,
                 num_extensions: int = _NUM_EXTENSIONS,
                 context_manager_blocking: bool = True,
                 context_manager_timeout: float = -1,
                 ) -> None:
        '''Initialize an AIORedlock.

        Keyword arguments:
            key -- a string that identifies your resource
            masters -- the asyncio Redis clients used to achieve quorum for this
                AIORedlock's state
            raise_on_redis_errors -- whether to raise the QuorumIsImplssible
                exception when too many Redis masters throw errors
            auto_release_time -- the timeout in seconds by which to
                automatically release this AIORedlock, unless it's already been
                released
            num_extensions -- the number of times that this AIORedlock's lease
                can be extended
            context_manager_blocking -- when using this AIORedlock as a context
                manager, whether to block when acquiring
            context_manager_timeout -- if context_manager_blocking, how long to
                wait when acquiring before giving up and raising the
                QuorumNotAchieved exception
        '''
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

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    async def __acquire_master(self, master: AIORedis) -> bool:  # type: ignore
        acquired = await master.set(
            self.key,
            self._uuid,
            px=int(self.auto_release_time * 1000),
            nx=True,
        )
        return bool(acquired)

    async def __acquired_master(self, master: AIORedis) -> int:  # type: ignore
        if self._uuid:
            ttl: int = await self._acquired_script(  # type: ignore
                keys=(self.key,),
                args=(self._uuid,),
                client=master,
            )
        else:
            ttl = 0
        return ttl

    async def __extend_master(self, master: AIORedis) -> bool:  # type: ignore
        auto_release_time_ms = int(self.auto_release_time * 1000)
        extended = await self._extend_script(  # type: ignore
            keys=(self.key,),
            args=(self._uuid, auto_release_time_ms),
            client=master,
        )
        return bool(extended)

    async def __release_master(self, master: AIORedis) -> bool:  # type: ignore
        released = await self._release_script(  # type: ignore
            keys=(self.key,),
            args=(self._uuid,),
            client=master,
        )
        return bool(released)

    def __drift(self) -> float:
        return self.auto_release_time * self._CLOCK_DRIFT_FACTOR + .002

    async def _acquire_masters(self,
                               *,
                               raise_on_redis_errors: bool | None = None,
                               ) -> bool:
        self._uuid = str(uuid.uuid4())
        self._extension_num = 0

        with ContextTimer() as timer:
            num_masters_acquired, redis_errors = 0, []
            coros = [self.__acquire_master(master) for master in self.masters]
            for coro in asyncio.as_completed(coros):
                try:
                    num_masters_acquired += await coro
                except RedisError as error:
                    redis_errors.append(error)
                    logger.exception(
                        '%s.__acquire_masters() caught %s',
                        self.__class__.__qualname__,
                        error.__class__.__qualname__,
                    )
            if num_masters_acquired > len(self.masters) // 2:
                validity_time = self.auto_release_time
                validity_time -= self.__drift()
                validity_time -= timer.elapsed() / 1000
                if validity_time > 0:
                    return True

        with contextlib.suppress(ReleaseUnlockedLock):
            await self.__release()
        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        return False

    __acquire_masters = _acquire_masters

    async def acquire(self,
                      *,
                      blocking: bool = True,
                      timeout: float = -1,
                      raise_on_redis_errors: bool | None = None,
                      ) -> bool:
        '''Lock the lock.

        If blocking is True and timeout is -1, then wait for as long as
        necessary to acquire the lock.  Return True.

            >>> import asyncio
            >>> from redis.asyncio import Redis as AIORedis
            >>> async def main():
            ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
            ...     shower = AIORedlock(key='shower', masters={aioredis})
            ...     if await shower.acquire():
            ...         # Critical section - no other coroutine can enter while we hold the lock.
            ...         print(f"shower is {'occupied' if await shower.locked() else 'available'}")
            ...         await shower.release()
            ...     print(f"shower is {'occupied' if await shower.locked() else 'available'}")
            >>> asyncio.run(main(), debug=True)
            shower is occupied
            shower is available

        If blocking is True and timeout is not -1, then wait for up to timeout
        seconds to acquire the lock.  Return True if the lock was acquired;
        False if it wasn't.

            >>> async def main():
            ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
            ...     shower_lock_1 = AIORedlock(key='shower', masters={aioredis}, auto_release_time=.2)
            ...     shower_lock_2 = AIORedlock(key='shower', masters={aioredis}, auto_release_time=.2)
            ...     if await shower_lock_1.acquire():
            ...         print('shower_lock_1 acquired')
            ...     if await shower_lock_2.acquire(timeout=.5):
            ...         print('shower_lock_2 acquired')
            ...         await shower_lock_2.release()
            >>> asyncio.run(main(), debug=True)
            shower_lock_1 acquired
            shower_lock_2 acquired

            >>> async def main():
            ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
            ...     shower_lock_1 = AIORedlock(key='shower', masters={aioredis})
            ...     shower_lock_2 = AIORedlock(key='shower', masters={aioredis})
            ...     if await shower_lock_1.acquire():
            ...         print('shower_lock_1 acquired')
            ...     if not await shower_lock_2.acquire(timeout=.2):
            ...         print('shower_lock_2 not acquired')
            ...     await shower_lock_1.release()
            >>> asyncio.run(main(), debug=True)
            shower_lock_1 acquired
            shower_lock_2 not acquired

        If blocking is False and timeout is -1, then try just once right now to
        acquire the lock.  Return True if the lock was acquired; False if it
        wasn't.

            >>> async def main():
            ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
            ...     shower_lock_1 = AIORedlock(key='shower', masters={aioredis})
            ...     shower_lock_2 = AIORedlock(key='shower', masters={aioredis})
            ...     if await shower_lock_1.acquire():
            ...         print('shower_lock_1 acquired')
            ...     if not await shower_lock_2.acquire(blocking=False):
            ...         print('shower_lock_2 not acquired')
            ...     await shower_lock_1.release()
            >>> asyncio.run(main(), debug=True)
            shower_lock_1 acquired
            shower_lock_2 not acquired
        '''
        acquire_masters = functools.partial(
            self.__acquire_masters,
            raise_on_redis_errors=raise_on_redis_errors,
        )

        if blocking:
            enqueued = False
            with ContextTimer() as timer:
                while timeout == -1 or timer.elapsed() / 1000 < timeout:
                    if await acquire_masters():
                        if enqueued:
                            self.__log_time_enqueued(timer, acquired=True)
                        return True
                    enqueued = True
                    delay = random.uniform(0, self._RETRY_DELAY)  # nosec
                    await asyncio.sleep(delay)
            if enqueued:  # pragma: no cover
                self.__log_time_enqueued(timer, acquired=False)
            return False  # pragma: no cover

        if timeout == -1:
            return await acquire_masters()

        raise ValueError("can't specify a timeout for a non-blocking call")

    __acquire = acquire

    def __log_time_enqueued(self, timer: ContextTimer, *, acquired: bool) -> None:
        key_suffix = self.key.split(':', maxsplit=1)[1]
        time_enqueued = math.ceil(timer.elapsed())
        logger.info(
            'source=pottery sample#aioredlock.enqueued.%s=%dms sample#aioredlock.acquired.%s=%d',
            key_suffix,
            time_enqueued,
            key_suffix,
            acquired,
        )

    async def locked(self,
                     *,
                     raise_on_redis_errors: bool | None = None,
                     ) -> float:
        '''How much longer we'll hold the lock (unless we extend or release it).

        If we don't currently hold the lock, then this method returns 0.

            >>> async def main():
            ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
            ...     shower_lock = AIORedlock(key='shower', masters={aioredis})
            ...     print(await shower_lock.locked())
            ...     async with shower_lock:
            ...         print(math.ceil(await shower_lock.locked()))
            ...     print(await shower_lock.locked())
            >>> asyncio.run(main(), debug=True)
            0
            10
            0
        '''
        with ContextTimer() as timer:
            ttls, redis_errors = [], []
            coros = [self.__acquired_master(master) for master in self.masters]
            for coro in asyncio.as_completed(coros):
                try:
                    ttl = await coro / 1000
                except RedisError as error:
                    redis_errors.append(error)
                    logger.exception(
                        '%s.locked() caught %s',
                        self.__class__.__qualname__,
                        error.__class__.__qualname__,
                    )
                else:
                    if ttl:
                        ttls.append(ttl)
            if len(ttls) > len(self.masters) // 2:
                index = len(self.masters) // 2 - (not len(self.masters) % 2)
                validity_time: float = sorted(ttls)[index]
                validity_time -= self.__drift()
                validity_time -= timer.elapsed() / 1000
                return max(validity_time, 0)

        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        return 0

    async def extend(self,
                     *,
                     raise_on_redis_errors: bool | None = None,
                     ) -> None:
        '''Extend our hold on the lock (if we currently hold it).

            >>> async def main():
            ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
            ...     shower_lock = AIORedlock(key='shower', masters={aioredis})
            ...     await shower_lock.acquire()
            ...     print(math.ceil(await shower_lock.locked()))
            ...     await asyncio.sleep(1)
            ...     print(math.ceil(await shower_lock.locked()))
            ...     await shower_lock.extend()
            ...     print(math.ceil(await shower_lock.locked()))
            ...     await shower_lock.release()
            >>> asyncio.run(main(), debug=True)
            10
            9
            10
        '''
        if self._extension_num >= self.num_extensions:
            raise TooManyExtensions(self.key, self.masters)

        num_masters_extended, redis_errors = 0, []
        coros = [self.__extend_master(master) for master in self.masters]
        for coro in asyncio.as_completed(coros):
            try:
                num_masters_extended += await coro
            except RedisError as error:
                redis_errors.append(error)
                logger.exception(
                    '%s.extend() caught %s',
                    self.__class__.__qualname__,
                    error.__class__.__qualname__,
                )
        if num_masters_extended > len(self.masters) // 2:
            self._extension_num += 1
            return

        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        raise ExtendUnlockedLock(
            self.key,
            self.masters,
            redis_errors=redis_errors,
        )

    async def release(self,
                      *,
                      raise_on_redis_errors: bool | None = None,
                      ) -> None:
        '''Unlock the lock.

            >>> async def main():
            ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
            ...     shower_lock = AIORedlock(key='shower', masters={aioredis})
            ...     await shower_lock.acquire()
            ...     print(bool(await shower_lock.locked()))
            ...     await shower_lock.release()
            ...     print(bool(await shower_lock.locked()))
            >>> asyncio.run(main(), debug=True)
            True
            False
        '''
        num_masters_released, redis_errors = 0, []
        coros = [self.__release_master(master) for master in self.masters]
        for coro in asyncio.as_completed(coros):
            try:
                num_masters_released += await coro
            except RedisError as error:
                redis_errors.append(error)
                logger.exception(
                    '%s.release() caught %s',
                    self.__class__.__qualname__,
                    error.__class__.__qualname__,
                )
        if num_masters_released > len(self.masters) // 2:
            return

        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        raise ReleaseUnlockedLock(
            self.key,
            self.masters,
            redis_errors=redis_errors,
        )

    __release = release

    async def __aenter__(self) -> AIORedlock:
        '''You can use an AIORedlock as a context manager.

            >>> async def main():
            ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
            ...     shower = AIORedlock(key='shower', masters={aioredis})
            ...     async with shower:
            ...         print(f"shower is {'occupied' if await shower.locked() else 'available'}")
            ...         # Critical section - no other coroutine can enter while we hold the lock.
            ...     print(f"shower is {'occupied' if await shower.locked() else 'available'}")
            >>> asyncio.run(main(), debug=True)
            shower is occupied
            shower is available
        '''
        acquired = await self.__acquire(
            blocking=self.context_manager_blocking,
            timeout=self.context_manager_timeout,
        )
        if acquired:
            return self
        raise QuorumNotAchieved(self.key, self.masters)

    async def __aexit__(self,
                        exc_type: Type[BaseException] | None,
                        exc_value: BaseException | None,
                        traceback: TracebackType | None,
                        ) -> None:
        '''You can use an AIORedlock as a context manager.

            >>> async def main():
            ...     aioredis = AIORedis.from_url('redis://localhost:6379/1')
            ...     shower = AIORedlock(key='shower', masters={aioredis})
            ...     async with shower:
            ...         print(f"shower is {'occupied' if await shower.locked() else 'available'}")
            ...         # Critical section - no other coroutine can enter while we hold the lock.
            ...     print(f"shower is {'occupied' if await shower.locked() else 'available'}")
            >>> asyncio.run(main(), debug=True)
            shower is occupied
            shower is available
        '''
        await self.__release()

    def __repr__(self) -> str:
        return f'<{self.__class__.__qualname__} key={self.key}>'
