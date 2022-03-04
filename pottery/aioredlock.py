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
'Asynchronous distributed Redis-powered lock.'


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
                 masters: Iterable[AIORedis] = frozenset(),
                 raise_on_redis_errors: bool = False,
                 auto_release_time: float = _AUTO_RELEASE_TIME,
                 num_extensions: int = _NUM_EXTENSIONS,
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
        released: bool = await self._release_script(  # type: ignore
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
                        self.__class__.__name__,
                        error.__class__.__name__,
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
                            self.__log_time_enqueued(timer, True)
                        return True
                    enqueued = True
                    delay = random.uniform(0, self._RETRY_DELAY)  # nosec
                    await asyncio.sleep(delay)
            if enqueued:  # pragma: no cover
                self.__log_time_enqueued(timer, False)
            return False  # pragma: no cover

        if timeout == -1:
            return await acquire_masters()

        raise ValueError("can't specify a timeout for a non-blocking call")

    __acquire = acquire

    def __log_time_enqueued(self, timer: ContextTimer, acquired: bool) -> None:
        key_suffix = self.key.split(':', maxsplit=1)[1]
        time_enqueued = math.ceil(timer.elapsed())
        logger.info(
            'source=pottery sample#redlock.enqueued.%s=%dms sample#redlock.acquired.%s=%d',
            key_suffix,
            time_enqueued,
            key_suffix,
            acquired,
        )

    async def locked(self,
                     *,
                     raise_on_redis_errors: bool | None = None,
                     ) -> float:
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
                        self.__class__.__name__,
                        error.__class__.__name__,
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
                    self.__class__.__name__,
                    error.__class__.__name__,
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
        num_masters_released, redis_errors = 0, []
        coros = [self.__release_master(master) for master in self.masters]
        for coro in asyncio.as_completed(coros):
            try:
                num_masters_released += await coro
            except RedisError as error:
                redis_errors.append(error)
                logger.exception(
                    '%s.release() caught %s',
                    self.__class__.__name__,
                    error.__class__.__name__,
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
        await self.release()
