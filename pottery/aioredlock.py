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
import uuid
from types import TracebackType
from typing import ClassVar
from typing import Iterable
from typing import Type

from redis import RedisError
from redis.asyncio import Redis as AIORedis  # type: ignore
# TODO: When we drop support for Python 3.7, change the following import to:
#   from typing import Literal
from typing_extensions import Literal

from .base import AIOPrimitive
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
        '_uuid',
        '_extension_num',
    )

    _KEY_PREFIX: ClassVar[str] = Redlock._KEY_PREFIX

    def __init__(self,  # type: ignore
                 *,
                 key: str,
                 masters: Iterable[AIORedis] = frozenset(),
                 auto_release_time: float = Redlock._AUTO_RELEASE_TIME,
                 num_extensions: int = Redlock._NUM_EXTENSIONS,
                 ) -> None:
        super().__init__(key=key, masters=masters)
        self.auto_release_time = auto_release_time
        self.num_extensions = num_extensions
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
        return self.auto_release_time * Redlock._CLOCK_DRIFT_FACTOR + .002

    async def acquire(self) -> Literal[True]:
        self._uuid = str(uuid.uuid4())
        self._extension_num = 0

        with ContextTimer() as timer:
            coros = (self.__acquire_master(master) for master in self.masters)
            results = await asyncio.gather(*coros, return_exceptions=True)
            num_masters_acquired, redis_errors = 0, []
            for result in results:
                if isinstance(result, RedisError):
                    redis_errors.append(result)
                elif isinstance(result, bool):
                    num_masters_acquired += result
                    if num_masters_acquired > len(self.masters) // 2:
                        validity_time = self.auto_release_time
                        validity_time -= self.__drift()
                        validity_time -= timer.elapsed() / 1000
                        if validity_time > 0:
                            return True

        with contextlib.suppress(ReleaseUnlockedLock):
            await self.__release()
        raise QuorumNotAchieved(self.key, self.masters)

    async def locked(self) -> float:
        with ContextTimer() as timer:
            coros = (self.__acquired_master(master) for master in self.masters)
            results = await asyncio.gather(*coros, return_exceptions=True)
            ttls, redis_errors = [], []
            for result in results:
                if isinstance(result, RedisError):
                    redis_errors.append(result)
                elif isinstance(result, int):
                    ttls.append(result)
                    if len(ttls) > len(self.masters) // 2:
                        validity_time: float = min(ttls)
                        validity_time -= self.__drift()
                        validity_time -= timer.elapsed() / 1000
                        return max(validity_time, 0)
        return 0

    async def extend(self) -> None:
        if self._extension_num >= self.num_extensions:
            raise TooManyExtensions(self.key, self.masters)

        coros = (self.__extend_master(master) for master in self.masters)
        results = await asyncio.gather(*coros, return_exceptions=True)
        num_masters_extended, redis_errors = 0, []
        for result in results:
            if isinstance(result, RedisError):
                redis_errors.append(result)
            elif isinstance(result, bool):
                num_masters_extended += result
                if num_masters_extended > len(self.masters) // 2:
                    self._extension_num += 1
                    return

        raise ExtendUnlockedLock(self.key, self.masters)

    async def release(self) -> None:
        coros = (self.__release_master(master) for master in self.masters)
        results = await asyncio.gather(*coros, return_exceptions=True)
        num_masters_released, redis_errors = 0, []
        for result in results:
            if isinstance(result, RedisError):
                redis_errors.append(result)
            elif isinstance(result, bool):
                num_masters_released += result
                if num_masters_released > len(self.masters) // 2:
                    return

        raise ReleaseUnlockedLock(self.key, self.masters)

    __release = release

    async def __aenter__(self) -> AIORedlock:
        await self.acquire()
        return self

    async def __aexit__(self,
                        exc_type: Type[BaseException] | None,
                        exc_value: BaseException | None,
                        traceback: TracebackType | None,
                        ) -> None:
        await self.release()
