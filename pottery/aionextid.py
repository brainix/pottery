# --------------------------------------------------------------------------- #
#   aionextid.py                                                              #
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
'Asynchronous distributed Redis-powered monotonically increasing ID generator.'


# TODO: Remove the following import after deferred evaluation of annotations
# because the default.
#   1. https://docs.python.org/3/whatsnew/3.7.html#whatsnew37-pep563
#   2. https://www.python.org/dev/peps/pep-0563/
#   3. https://www.python.org/dev/peps/pep-0649/
from __future__ import annotations

import asyncio
import contextlib
from typing import ClassVar
from typing import Iterable

from redis import RedisError
from redis.asyncio import Redis as AIORedis  # type: ignore

from .base import AIOPrimitive
from .base import logger
from .exceptions import QuorumNotAchieved
from .nextid import NextID
from .nextid import Scripts


class AIONextID(Scripts, AIOPrimitive):
    'Async distributed Redis-powered monotonically increasing ID generator.'

    __slots__ = ('num_tries',)

    _KEY_PREFIX: ClassVar[str] = NextID._KEY_PREFIX

    def __init__(self,  # type: ignore
                 *,
                 key: str = 'current',
                 masters: Iterable[AIORedis] = frozenset(),
                 num_tries: int = NextID._NUM_TRIES,
                 ) -> None:
        'Initialize an AIONextID ID generator.'
        super().__init__(key=key, masters=masters)
        self.num_tries = num_tries

    def __aiter__(self) -> AIONextID:
        return self  # pragma: no cover

    async def __anext__(self) -> int:
        for _ in range(self.num_tries):
            with contextlib.suppress(QuorumNotAchieved):
                next_id = await self.__get_current_ids() + 1
                await self.__set_current_ids(next_id)
                return next_id
        raise QuorumNotAchieved(self.key, self.masters)

    async def __get_current_id(self, master: AIORedis) -> int:  # type: ignore
        current_id: int = await master.get(self.key)
        return current_id

    async def __set_current_id(self, master: AIORedis, value: int) -> bool:  # type: ignore
        current_id: int | None = await self._set_id_script(  # type: ignore
            keys=(self.key,),
            args=(value,),
            client=master,
        )
        return current_id == value

    async def __reset_current_id(self, master: AIORedis) -> int:  # type: ignore
        await master.delete(self.key)

    async def __get_current_ids(self) -> int:
        current_ids, redis_errors = [], []
        coros = [self.__get_current_id(master) for master in self.masters]
        for coro in asyncio.as_completed(coros):  # type: ignore
            try:
                current_id = int(await coro or b'0')
            except RedisError as error:
                redis_errors.append(error)
                logger.exception(
                    '%s.__get_current_ids() caught %s',
                    self.__class__.__qualname__,
                    error.__class__.__qualname__,
                )
            else:
                current_ids.append(current_id)
        if len(current_ids) > len(self.masters) // 2:
            return max(current_ids)
        raise QuorumNotAchieved(
            self.key,
            self.masters,
            redis_errors=redis_errors,
        )

    async def __set_current_ids(self, value: int) -> None:
        num_masters_set, redis_errors = 0, []
        coros = [self.__set_current_id(master, value) for master in self.masters]
        for coro in asyncio.as_completed(coros):
            try:
                num_masters_set += await coro
            except RedisError as error:
                redis_errors.append(error)
                logger.exception(
                    '%s.__set_current_ids() caught %s',
                    self.__class__.__qualname__,
                    error.__class__.__qualname__,
                )
        if num_masters_set > len(self.masters) // 2:
            return
        raise QuorumNotAchieved(
            self.key,
            self.masters,
            redis_errors=redis_errors,
        )

    async def reset(self) -> None:
        num_masters_reset, redis_errors = 0, []
        coros = [self.__reset_current_id(master) for master in self.masters]
        for coro in asyncio.as_completed(coros):
            try:
                await coro
            except RedisError as error:
                redis_errors.append(error)
                logger.exception(
                    '%s.reset() caught %s',
                    self.__class__.__qualname__,
                    error.__class__.__qualname__,
                )
            else:
                num_masters_reset += 1
        if num_masters_reset > len(self.masters) // 2:
            return
        raise QuorumNotAchieved(
            self.key,
            self.masters,
            redis_errors=redis_errors,
        )

    def __repr__(self) -> str:
        return f'<{self.__class__.__qualname__} key={self.key}>'
