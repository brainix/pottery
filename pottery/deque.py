# --------------------------------------------------------------------------- #
#   deque.py                                                                  #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import collections
from typing import Iterable
from typing import Optional

from redis import Redis
from redis.client import Pipeline

from .base import JSONTypes
from .list import RedisList


class RedisDeque(RedisList, collections.deque):  # type: ignore
    'Redis-backed container compatible with collections.deque.'

    # Method overrides:

    def __init__(self,
                 iterable: Iterable[JSONTypes] = tuple(),
                 maxlen: Optional[int] = None,
                 *,
                 redis: Optional[Redis] = None,
                 key: Optional[str] = None,
                 ) -> None:
        'Initialize a RedisDeque.  O(n)'
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
            else:  # pragma: no cover
                iterable = tuple()
        super()._populate(pipeline, iterable)

    @property
    def maxlen(self) -> Optional[int]:
        return self._maxlen

    @maxlen.setter
    def maxlen(self, value: int) -> None:
        raise AttributeError(
            f"attribute 'maxlen' of '{self.__class__.__name__}' objects is not "
            'writable'
        )

    def insert(self, index: int, value: JSONTypes) -> None:
        'Insert an element into a RedisDeque before the given index.  O(n)'
        if self.maxlen is not None and len(self) >= self.maxlen:
            raise IndexError(
                f'{self.__class__.__name__} already at its maximum size'
            )
        else:
            return super()._insert(index, value)

    def append(self, value: JSONTypes) -> None:
        'Add an element to the right side of the RedisDeque.  O(1)'
        self.__extend((value,), right=True)

    def appendleft(self, value: JSONTypes) -> None:
        'Add an element to the left side of the RedisDeque.  O(1)'
        self.__extend((value,), right=False)

    def extend(self, values: Iterable[JSONTypes]) -> None:
        'Extend a RedisList by appending elements from the iterable.  O(1)'
        self.__extend(values, right=True)

    def extendleft(self, values: Iterable[JSONTypes]) -> None:
        '''Extend a RedisList by prepending elements from the iterable.  O(1)

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
            encoded_values = [self._encode(value) for value in values]
            len_ = len(self) + len(encoded_values)
            pipeline.multi()
            push_method = 'rpush' if right else 'lpush'
            getattr(pipeline, push_method)(self.key, *encoded_values)
            if self.maxlen is not None and len_ >= self.maxlen:
                if right:
                    trim_indices = len_-self.maxlen, len_
                else:
                    trim_indices = 0, self.maxlen-1
                pipeline.ltrim(self.key, *trim_indices)

    def pop(self) -> JSONTypes:  # type: ignore
        return super().pop()

    def popleft(self) -> JSONTypes:
        return super().pop(0)

    def rotate(self, n: int = 1) -> None:
        '''Rotate the RedisDeque n steps to the right (default n=1).

        If n is negative, rotates left.
        '''
        if n:
            with self._watch() as pipeline:
                push_method = 'lpush' if n > 0 else 'rpush'
                values = self[-n:] if n > 0 else self[:-n]
                encoded_values = (self._encode(element) for element in values)
                trim_indices = (0, len(self)-n) if n > 0 else (-n, len(self))

                pipeline.multi()
                getattr(pipeline, push_method)(self.key, *encoded_values)
                pipeline.ltrim(self.key, *trim_indices)

    # Methods required for Raj's sanity:

    def __repr__(self) -> str:
        'Return the string representation of a RedisDeque.  O(n)'
        encoded = self.redis.lrange(self.key, 0, -1)
        values = [self._decode(value) for value in encoded]
        repr = self.__class__.__name__ + '(' + str(values)
        if self.maxlen is not None:
            repr += f', maxlen={self.maxlen}'
        repr += ')'
        return repr
