# --------------------------------------------------------------------------- #
#   exceptions.py                                                             #
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


from typing import Iterable
from typing import Optional

from redis import Redis
from redis import RedisError


class PotteryError(Exception):
    'Base exception class for Pottery containers.'

    def __init__(self, redis: Redis, key: Optional[str]) -> None:
        self._redis = redis
        self._key = key

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(redis={self._redis}, key='{self._key}')"
        )

    def __str__(self) -> str:
        return f"redis={self._redis} key='{self._key}'"

class KeyExistsError(PotteryError):
    'Initializing a container on a Redis key that already exists.'

class RandomKeyError(PotteryError, RuntimeError):
    "Can't create a random Redis key; all of our attempts already exist."

    def __init__(self, redis: Redis) -> None:
        super().__init__(redis, None)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(redis={self._redis})"

    def __str__(self) -> str:
        return f'redis={self._redis}'


class PrimitiveError(Exception):
    'Base exception class for distributed primitives.'

    def __init__(self,
                 key: str,
                 masters: Iterable[Redis],
                 *,
                 redis_errors: Iterable[RedisError] = tuple(),
                 ) -> None:
        self._key = key
        self._masters = masters
        self._redis_errors = redis_errors

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(key='{self._key}', "
            f"masters={list(self._masters)}, "
            f"redis_errors={list(self._redis_errors)})"
        )

    def __str__(self) -> str:
        return (
            f"key='{self._key}', "
            f"masters={list(self._masters)}, "
            f"redis_errors={list(self._redis_errors)}"
        )

class QuorumNotAchieved(PrimitiveError, RuntimeError):
    'Consensus-based algorithm could not achieve quorum.'

class TooManyExtensions(PrimitiveError, RuntimeError):
    'Redlock has been extended too many times.'

class ExtendUnlockedLock(PrimitiveError, RuntimeError):
    'Attempting to extend an unlocked Redlock.'

class ReleaseUnlockedLock(PrimitiveError, RuntimeError):
    'Attempting to release an unlocked Redlock.'

class QuorumIsImpossible(PrimitiveError, RuntimeError):
    'Too many Redis masters threw RedisErrors; quorum can not be achieved.'


class PotteryWarning(Warning):
    'Base warning class for Pottery containers.'

class InefficientAccessWarning(PotteryWarning):
    'Doing an O(n) Redis operation.'
