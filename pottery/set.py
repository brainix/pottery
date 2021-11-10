# --------------------------------------------------------------------------- #
#   set.py                                                                    #
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


import collections.abc
import itertools
from typing import Any
from typing import Iterable
from typing import List
from typing import NoReturn
from typing import Optional
from typing import Set
from typing import Tuple
from typing import cast

from redis import Redis
from redis.client import Pipeline
from typing_extensions import Literal

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
        'Initialize the RedisSet.  O(n)'
        super().__init__(redis=redis, key=key)
        if iterable:
            with self._watch(iterable) as pipeline:
                if pipeline.exists(self.key):
                    raise KeyExistsError(self.redis, self.key)
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
        'Return the number of elements in the RedisSet.  O(1)'
        return self.redis.scard(self.key)

    def add(self, value: JSONTypes) -> None:
        'Add an element to the RedisSet.  O(1)'
        self.redis.sadd(self.key, self._encode(value))

    def discard(self, value: JSONTypes) -> None:
        'Remove an element from the RedisSet.  O(1)'
        self.redis.srem(self.key, self._encode(value))

    # Methods required for Raj's sanity:

    def __repr__(self) -> str:
        'Return the string representation of the RedisSet.  O(n)'
        set_ = {self._decode(value) for value in self.redis.smembers(self.key)}
        return self.__class__.__name__ + str(set_)

    # Method overrides:

    # From collections.abc.MutableSet:
    def pop(self) -> JSONTypes:
        'Remove and return an element from the RedisSet().  O(1)'
        encoded_value = self.redis.spop(self.key)
        if encoded_value is None:
            raise KeyError('pop from an empty set')
        return self._decode(cast(bytes, encoded_value))

    # From collections.abc.MutableSet:
    def remove(self, value: JSONTypes) -> None:
        'Remove an element from the RedisSet().  O(1)'
        if not self.redis.srem(self.key, self._encode(value)):
            raise KeyError(value)

    # From collections.abc.Set:
    def isdisjoint(self, other: Iterable[Any]) -> bool:
        'Return True if two sets have a null intersection.  O(n)'
        return not self.__intersection(other)

    # Where does this method come from?
    def issubset(self, other: Iterable[Any]) -> bool:
        with self._watch(other):
            if not isinstance(other, collections.abc.Set):
                other = frozenset(other)
            return self <= other

    # Where does this method come from?
    def issuperset(self, other: Iterable[Any]) -> bool:
        with self._watch(other):
            if not isinstance(other, collections.abc.Set):
                other = frozenset(other)
            return self >= other

    # Where does this method come from?
    def union(self, *others: Iterable[Any]) -> Set[Any]:
        return self.__set_op(
            *others,
            redis_method='sunion',
            set_method='union',
        )

    # Where does this method come from?
    def intersection(self, *others: Iterable[Any]) -> Set[Any]:
        return self.__set_op(
            *others,
            redis_method='sinter',
            set_method='intersection',
        )

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __intersection = intersection

    # Where does this method come from?
    def difference(self, *others: Iterable[Any]) -> Set[Any]:
        return self.__set_op(
            *others,
            redis_method='sdiff',
            set_method='difference',
        )

    def __set_op(self,
                 *others: Iterable[Any],
                 redis_method: Literal['sunion', 'sinter', 'sdiff'],
                 set_method: Literal['union', 'intersection', 'difference'],
                 ) -> Set[Any]:
        if self._same_redis(*others):
            method = getattr(self.redis, redis_method)
            keys = (self.key, *(cast(RedisSet, other).key for other in others))
            encoded_values = method(*keys)
            decoded_values = {
                self._decode(cast(bytes, value)) for value in encoded_values
            }
            return decoded_values
        with self._watch(*others):
            set_ = set(self)
            method = getattr(set_, set_method)
            return method(*others)

    # Where does this method come from?
    def symmetric_difference(self, other: Iterable[Any]) -> NoReturn:  # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def update(self, *others: Iterable[JSONTypes]) -> None:
        self.__update(
            *others,
            redis_method='sunionstore',
            pipeline_method='sadd',
        )

    # Where does this method come from?
    def intersection_update(self, *others: Iterable[JSONTypes]) -> NoReturn:  # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def difference_update(self, *others: Iterable[JSONTypes]) -> None:
        self.__update(
            *others,
            redis_method='sdiffstore',
            pipeline_method='srem',
        )

    def __update(self,
                 *others: Iterable[JSONTypes],
                 redis_method: Literal['sunionstore', 'sdiffstore'],
                 pipeline_method: Literal['sadd', 'srem'],
                 ) -> None:
        if not others:
            return
        if self._same_redis(*others):
            method = getattr(self.redis, redis_method)
            keys = (
                self.key,
                self.key,
                *(cast(RedisSet, other).key for other in others)
            )
            method(*keys)
        else:
            with self._watch(*others) as pipeline:
                encoded_values = set()
                for value in itertools.chain(*others):
                    encoded_values.add(self._encode(value))
                if encoded_values:
                    pipeline.multi()
                    method = getattr(pipeline, pipeline_method)
                    method(self.key, *encoded_values)

    # Where does this method come from?
    def symmetric_difference_update(self, other: Iterable[JSONTypes]) -> NoReturn:  # pragma: no cover
        raise NotImplementedError
