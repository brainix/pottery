# --------------------------------------------------------------------------- #
#   exceptions.py                                                             #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


from typing import Iterable
from typing import Optional

from redis import Redis


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

    def __init__(self, masters: Iterable[Redis], key: str) -> None:
        self._masters = masters
        self._key = key

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(masters={list(self._masters)}, "
            f"key='{self._key}')"
        )

    def __str__(self) -> str:
        return f"masters={list(self._masters)}, key='{self._key}'"

class QuorumNotAchieved(PrimitiveError, RuntimeError):
    ...

class TooManyExtensions(PrimitiveError, RuntimeError):
    ...

class ExtendUnlockedLock(PrimitiveError, RuntimeError):
    ...

class ReleaseUnlockedLock(PrimitiveError, RuntimeError):
    ...
