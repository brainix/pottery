# --------------------------------------------------------------------------- #
#   set.py                                                                    #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import collections.abc
import itertools
from typing import Any
from typing import Iterable
from typing import List
from typing import NoReturn
from typing import Optional
from typing import Tuple

from redis import Redis
from redis.client import Pipeline

from .base import Base
from .base import Iterable_
from .base import JSONTypes
from .exceptions import KeyExistsError


class RedisSet(Base, Iterable_, collections.abc.MutableSet):
    'Redis-backed container compatible with Python sets.'

    def __init__(self,
                 iterable: Iterable[JSONTypes] = tuple(),
                 *,
                 redis: Optional[Redis] = None,
                 key: Optional[str] = None,
                 ) -> None:
        'Initialize a RedisSet.  O(n)'
        super().__init__(redis=redis, key=key)
        if iterable:
            with self._watch(iterable) as pipeline:
                if pipeline.exists(self.key):
                    raise KeyExistsError(self.redis, self.key)
                else:
                    self._populate(pipeline, iterable)

    def _populate(self,
                  pipeline: Pipeline,
                  iterable: Iterable[JSONTypes] = tuple(),
                  ) -> None:
        encoded_values = {self._encode(value) for value in iterable}
        if encoded_values:  # pragma: no cover
            pipeline.multi()
            pipeline.sadd(self.key, *encoded_values)

    # Methods required by collections.abc.MutableSet:

    def __contains__(self, value: Any) -> bool:
        's.__contains__(element) <==> element in s.  O(1)'
        try:
            return self.redis.sismember(self.key, self._encode(value))
        except TypeError:
            return False

    def _scan(self, *, cursor: int = 0) -> Tuple[int, List[bytes]]:
        return self.redis.sscan(self.key, cursor=cursor)

    def __len__(self) -> int:
        'Return the number of elements in a RedisSet.  O(1)'
        return self.redis.scard(self.key)

    def add(self, value: JSONTypes) -> None:
        'Add an element to a RedisSet.  O(1)'
        self.redis.sadd(self.key, self._encode(value))

    def discard(self, value: JSONTypes) -> None:
        'Remove an element from a RedisSet.  O(1)'
        self.redis.srem(self.key, self._encode(value))

    # Methods required for Raj's sanity:

    def __repr__(self) -> str:
        'Return the string representation of a RedisSet.  O(n)'
        set_ = {self._decode(value) for value in self.redis.smembers(self.key)}  # type: ignore
        return self.__class__.__name__ + str(set_)

    # Method overrides:

    # From collections.abc.MutableSet:
    def pop(self) -> JSONTypes:
        'Remove and return an element from a RedisSet().  O(1)'
        encoded_value = self.redis.spop(self.key)
        if encoded_value is None:
            raise KeyError('pop from an empty set')
        else:
            return self._decode(encoded_value)

    # From collections.abc.MutableSet:
    def remove(self, value: JSONTypes) -> None:
        'Remove an element from a RedisSet().  O(1)'
        if not self.redis.srem(self.key, self._encode(value)):
            raise KeyError(value)

    # From collections.abc.Set:
    def isdisjoint(self, other: Iterable[Any]) -> bool:
        'Return True if two sets have a null intersection.  O(n)'
        if (
            isinstance(other, self.__class__)
            and self.redis.connection_pool == other.redis.connection_pool  # NoQA: W503
        ):
            disjoint = not self.redis.sinter(self.key, other.key)
        else:
            with self._watch(other):
                disjoint = super().isdisjoint(other)
        return disjoint

    # Where does this method come from?
    def issubset(self, other: Iterable[Any]) -> NoReturn:  # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def issuperset(self, other: Iterable[Any]) -> NoReturn:  # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def union(self, *args: Iterable[Any]) -> NoReturn:  # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def intersection(self, *args: Iterable[Any]) -> NoReturn:  # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def difference(self, *args: Iterable[Any]) -> NoReturn:  # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def symmetric_difference(self, other: Iterable[Any]) -> NoReturn:  # pragma: no cover
        raise NotImplementedError

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    def __update(self,
                 *iterables: Iterable[JSONTypes],
                 redis_method: str,
                 ) -> None:
        with self._watch(*iterables) as pipeline:
            encoded_values = set()
            for value in itertools.chain(*iterables):
                encoded_values.add(self._encode(value))
            if encoded_values:
                pipeline.multi()
                getattr(pipeline, redis_method)(self.key, *encoded_values)

    # Where does this method come from?
    def update(self, *iterables: Iterable[JSONTypes]) -> None:
        self.__update(*iterables, redis_method='sadd')

    # Where does this method come from?
    def intersection_update(self, *args: Iterable[JSONTypes]) -> NoReturn:  # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def difference_update(self, *iterables: Iterable[JSONTypes]) -> None:
        self.__update(*iterables, redis_method='srem')

    # Where does this method come from?
    def symmetric_difference_update(self, other: Iterable[JSONTypes]) -> NoReturn:  # pragma: no cover
        raise NotImplementedError
