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

from types import TracebackType
from typing import ClassVar
from typing import Iterable
from typing import Literal
from typing import Type

from redis.asyncio import Redis as AIORedis  # type: ignore

from .base import AIOPrimitive
from .redlock import Redlock


class AIORedlock(AIOPrimitive):
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

    async def acquire(self) -> Literal[True]:
        # TODO: Fill me in.
        return True

    async def locked(self) -> float:
        # TODO: Fill me in.
        return 0

    async def extend(self) -> None:
        # TODO: Fill me in.
        ...

    async def release(self) -> None:
        # TODO: Fill me in.
        ...

    async def __aenter__(self) -> AIORedlock:
        await self.acquire()
        return self

    async def __aexit__(self,
                        exc_type: Type[BaseException] | None,
                        exc_value: BaseException | None,
                        traceback: TracebackType | None,
                        ) -> None:
        await self.release()
