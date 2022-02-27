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


from typing import ClassVar
from typing import Iterable

from redis.asyncio import Redis as AIORedis  # type: ignore

from .base import AIOPrimitive


class AIORedlock(AIOPrimitive):
    __slots__ = (
        'auto_release_time',
        'num_extensions',
        '_uuid',
        '_extension_num',
    )

    _KEY_PREFIX: ClassVar[str] = 'aioredlock'
    _AUTO_RELEASE_TIME: ClassVar[float] = 10
    _NUM_EXTENSIONS: ClassVar[int] = 3

    def __init__(self,  # type: ignore
                 *,
                 key: str,
                 masters: Iterable[AIORedis] = frozenset(),
                 raise_on_redis_errors: bool = False,
                 auto_release_time: float = _AUTO_RELEASE_TIME,
                 num_extensions: int = _NUM_EXTENSIONS,
                 ) -> None:
        super().__init__(
            key=key,
            masters=masters,
            raise_on_redis_errors=raise_on_redis_errors,
        )
        self.auto_release_time = auto_release_time
        self.num_extensions = num_extensions
        self._uuid = ''
        self._extension_num = 0
