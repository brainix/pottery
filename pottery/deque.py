# --------------------------------------------------------------------------- #
#   deque.py                                                                  #
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


# TODO: When we drop support for Python 3.9, remove the following import.  We
# only need it for X | Y union type annotations as of 2022-01-29.
from __future__ import annotations

import collections
import warnings
from typing import Iterable
from typing import Tuple
from typing import cast

from redis import Redis
from redis.client import Pipeline

from .annotations import JSONTypes
from .exceptions import InefficientAccessWarning
from .list import RedisList


class RedisDeque(RedisList, collections.deque):  # type: ignore
    'Redis-backed container compatible with collections.deque.'

    # Overrides:

    _ALLOWED_TO_EQUAL = collections.deque

    def __init__(self,
                 iterable: Iterable[JSONTypes] = tuple(),
                 maxlen: int | None = None,
                 *,
                 redis: Redis | None = None,
                 key: str = '',
                 ) -> None:
        'Initialize the RedisDeque.  O(n)'
        if maxlen is not None and not isinstance(maxlen, int):
            raise TypeError('an integer is required')
        self._maxlen = maxlen
        super().__init__(iterable, redis=redis, key=key)
        if not iterable and self.maxlen is not None and len(self) > self.maxlen:
            raise IndexError(
                f'persistent {self.__class__.__name__} beyond its maximum size'
            )

    def _populate(self,
                  pipeline: Pipeline,
                  iterable: Iterable[JSONTypes] = tuple(),
                  ) -> None:
        if self.maxlen is not None:
            if self.maxlen:
                iterable = tuple(iterable)[-self.maxlen:]
            else:
                # self.maxlen == 0.  Populate the RedisDeque with an empty
                # iterable.
                iterable = tuple()
        super()._populate(pipeline, iterable)

    @property
    def maxlen(self) -> int | None:
        return self._maxlen

    @maxlen.setter
    def maxlen(self, value: int) -> None:
        raise AttributeError(
            f"attribute 'maxlen' of '{self.__class__.__name__}' objects is not "
            'writable'
        )

    def insert(self, index: int, value: JSONTypes) -> None:
        'Insert an element into the RedisDeque before the given index.  O(n)'
        with self._watch() as pipeline:
            current_length = cast(int, pipeline.llen(self.key))  # Available since Redis 1.0.0
            if self.maxlen is not None and current_length >= self.maxlen:
                raise IndexError(
                    f'{self.__class__.__name__} already at its maximum size'
                )
            super()._insert(index, value, pipeline=pipeline)

    def append(self, value: JSONTypes) -> None:
        'Add an element to the right side of the RedisDeque.  O(1)'
        self.__extend((value,), right=True)

    def appendleft(self, value: JSONTypes) -> None:
        'Add an element to the left side of the RedisDeque.  O(1)'
        self.__extend((value,), right=False)

    def extend(self, values: Iterable[JSONTypes]) -> None:
        'Extend the RedisDeque by appending elements from the iterable.  O(1)'
        self.__extend(values, right=True)

    def extendleft(self, values: Iterable[JSONTypes]) -> None:
        '''Extend the RedisDeque by prepending elements from the iterable.  O(1)

        Note the order in which the elements are prepended from the iterable:

            >>> d = RedisDeque()
            >>> d.extendleft('abc')
            >>> d
            RedisDeque(['c', 'b', 'a'])
        '''
        self.__extend(values, right=False)

    def __extend(self,
                 values: Iterable[JSONTypes],
                 *,
                 right: bool = True,
                 ) -> None:
        with self._watch(values) as pipeline:
            push_method_name = 'rpush' if right else 'lpush'
            encoded_values = [self._encode(value) for value in values]
            len_ = cast(int, pipeline.llen(self.key)) + len(encoded_values)  # Available since Redis 1.0.0
            trim_indices: Tuple[int, int] | Tuple = tuple()
            if self.maxlen is not None and len_ >= self.maxlen:
                trim_indices = (len_-self.maxlen, len_) if right else (0, self.maxlen-1)

            pipeline.multi()  # Available since Redis 1.2.0
            push_method = getattr(pipeline, push_method_name)
            push_method(self.key, *encoded_values)
            if trim_indices:
                pipeline.ltrim(self.key, *trim_indices)  # Available since Redis 1.0.0

    def pop(self) -> JSONTypes:  # type: ignore
        return super().pop()

    def popleft(self) -> JSONTypes:
        return super().pop(0)

    def rotate(self, n: int = 1) -> None:
        '''Rotate the RedisDeque n steps to the right (default n=1).  O(n)

        If n is negative, rotates left.
        '''
        if not isinstance(n, int):
            raise TypeError(
                f"'{n.__class__.__name__}' object cannot be interpreted "
                'as an integer'
            )
        if n == 0:
            # Rotating 0 steps is a no-op.
            return

        with self._watch() as pipeline:
            if not self:
                # Rotating an empty RedisDeque is a no-op.
                return

            push_method_name = 'lpush' if n > 0 else 'rpush'  # Available since Redis 1.0.0
            values = self[-n:][::-1] if n > 0 else self[:-n]
            encoded_values = (self._encode(value) for value in values)
            trim_indices = (0, len(self)-1) if n > 0 else (-n, len(self)-1-n)

            pipeline.multi()  # Available since Redis 1.2.0
            push_method = getattr(pipeline, push_method_name)
            push_method(self.key, *encoded_values)
            pipeline.ltrim(self.key, *trim_indices)  # Available since Redis 1.0.0

    # Methods required for Raj's sanity:

    def __bool__(self) -> bool:
        'Whether the RedisDeque contains any elements.  O(1)'
        return bool(len(self))

    def __repr__(self) -> str:
        'Return the string representation of the RedisDeque.  O(n)'
        warnings.warn(
            cast(str, InefficientAccessWarning.__doc__),
            InefficientAccessWarning,
        )
        encoded_values = self.redis.lrange(self.key, 0, -1)  # Available since Redis 1.0.0
        values = [self._decode(value) for value in encoded_values]
        repr = self.__class__.__name__ + '(' + str(values)
        if self.maxlen is not None:
            repr += f', maxlen={self.maxlen}'
        repr += ')'
        return repr
