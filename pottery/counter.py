# --------------------------------------------------------------------------- #
#   counter.py                                                                #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import collections
import contextlib
import itertools
from typing import Callable
from typing import Iterable
from typing import Union
from typing import cast

from redis.client import Pipeline

from .base import JSONTypes
from .dict import RedisDict


InitIter = Iterable[JSONTypes]
InitArg = Union[InitIter, collections.Counter]


class RedisCounter(RedisDict, collections.Counter):
    'Redis-backed container compatible with collections.Counter.'

    # Method overrides:

    def _populate(self,  # type: ignore
                  arg: InitArg = tuple(),
                  *,
                  sign: int = +1,
                  **kwargs: int,
                  ) -> None:
        to_set = {}
        try:
            for key, value in cast(collections.Counter, arg).items():
                to_set[key] = sign * value
        except AttributeError:
            for key in arg:
                to_set[key] = to_set.get(key, self[key]) + sign
        for key, value in kwargs.items():
            original = self[key] if to_set.get(key, 0) == 0 else to_set[key]
            to_set[key] = original + sign * value
        to_set = {key: self[key] + value for key, value in to_set.items()}
        encoded_to_set = {
            self._encode(k): self._encode(v) for k, v in to_set.items()
        }
        if encoded_to_set:
            cast(Pipeline, self.redis).multi()
            self.redis.hset(self.key, mapping=encoded_to_set)  # type: ignore

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __populate = _populate

    def update(self, arg: InitArg = tuple(), **kwargs: int) -> None:  # type: ignore
        'Like dict.update() but add counts instead of replacing them.  O(n)'
        with self._watch(arg):
            self.__populate(arg, sign=+1, **kwargs)

    def subtract(self, arg: InitArg = tuple(), **kwargs: int) -> None:  # type: ignore
        'Like dict.update() but subtracts counts instead of replacing them.  O(n)'
        with self._watch(arg):
            self.__populate(arg, sign=-1, **kwargs)

    def __getitem__(self, key: JSONTypes) -> int:
        'c.__getitem__(key) <==> c.get(key, 0).  O(1)'
        try:
            value = cast(int, super().__getitem__(key))
        except KeyError:
            value = super().__missing__(key)  # type: ignore
        return value

    def __delitem__(self, key: JSONTypes) -> None:
        'c.__delitem__(key) <==> del c[key].  O(1)'
        with contextlib.suppress(KeyError):
            super().__delitem__(key)

    def __repr__(self) -> str:
        'Return the string representation of a RedisCounter.  O(n)'
        items = self.most_common()
        pairs = ("'{}': {}".format(key, value) for key, value in items)
        repr_ = ', '.join(pairs)
        return self.__class__.__name__ + '{' + repr_ + '}'

    def __math_op(self,
                  other: collections.Counter,
                  *,
                  method: Callable[[collections.Counter, collections.Counter], collections.Counter],
                  ) -> collections.Counter:
        with self._watch(other):
            counter = collections.Counter(self.elements())
            return method(counter, other)

    def __add__(self, other: collections.Counter) -> collections.Counter:
        "Return the addition our counts to other's counts, but keep only counts > 0.  O(n)"
        return self.__math_op(other, method=collections.Counter.__add__)

    def __sub__(self, other: collections.Counter) -> collections.Counter:
        "Return the subtraction other's counts from our counts, but keep only counts > 0.  O(n)"
        return self.__math_op(other, method=collections.Counter.__sub__)

    def __or__(self, other: collections.Counter) -> collections.Counter:
        "Return the max of our counts vs. other's counts (union), but keep only counts > 0.  O(n)"
        return self.__math_op(other, method=collections.Counter.__or__)

    def __and__(self, other: collections.Counter) -> collections.Counter:
        "Return the min of our counts vs. other's counts (intersection) but keep only counts > 0.  O(n)"
        return self.__math_op(other, method=collections.Counter.__and__)

    def __unary_op(self,
                   *,
                   test_func: Callable[[int], bool],
                   modifier_func: Callable[[int], int],
                   ) -> collections.Counter:
        counter: collections.Counter = collections.Counter()
        for key, value in self.items():
            if test_func(value):
                counter[key] = modifier_func(value)
        return counter

    def __pos__(self) -> collections.Counter:
        'Return our counts > 0.  O(n)'
        return self.__unary_op(
            test_func=lambda x: x > 0,
            modifier_func=lambda x: x,
        )

    def __neg__(self) -> collections.Counter:
        'Return the absolute value of our counts < 0.  O(n)'
        return self.__unary_op(
            test_func=lambda x: x < 0,
            modifier_func=lambda x: -x,
        )

    def __imath_op(self,
                   other: collections.Counter,
                   *,
                   sign: int = +1,
                   ) -> 'RedisCounter':
        with self._watch(other):
            to_set = {k: self[k] + sign * v for k, v in other.items()}
            to_del = {k for k, v in to_set.items() if v <= 0}
            to_del.update(
                k for k, v in self.items() if k not in to_set and v <= 0
            )
            encoded_to_set = {
                self._encode(k): self._encode(v) for k, v in to_set.items() if v
            }
            encoded_to_del = {self._encode(k) for k in to_del}
            if encoded_to_set or encoded_to_del:
                cast(Pipeline, self.redis).multi()
                if encoded_to_set:
                    self.redis.hset(self.key, mapping=encoded_to_set)  # type: ignore
                if encoded_to_del:
                    self.redis.hdel(self.key, *encoded_to_del)
        return self

    def __iadd__(self, other: collections.Counter) -> collections.Counter:
        'Same as __add__(), but in-place.  O(n)'
        return self.__imath_op(other, sign=+1)

    def __isub__(self, other: collections.Counter) -> collections.Counter:
        'Same as __sub__(), but in-place.  O(n)'
        return self.__imath_op(other, sign=-1)

    def __iset_op(self,
                  other: collections.Counter,
                  *,
                  method: Callable[[int, int], bool],
                  ) -> 'RedisCounter':
        with self._watch(other):
            to_set, to_del = {}, set()
            for k in itertools.chain(self, other):
                if method(self[k], other[k]):
                    to_set[k] = self[k]
                else:
                    to_set[k] = other[k]
                if to_set[k] <= 0:
                    del to_set[k]
                    to_del.add(k)
            if to_set or to_del:
                cast(Pipeline, self.redis).multi()
                if to_set:
                    encoded_to_set = {
                        self._encode(k): self._encode(v)
                        for k, v in to_set.items()
                    }
                    self.redis.hset(self.key, mapping=encoded_to_set)  # type: ignore
                if to_del:
                    encoded_to_del = {self._encode(k) for k in to_del}
                    self.redis.hdel(self.key, *encoded_to_del)
        return self

    def __ior__(self, other: collections.Counter) -> collections.Counter:
        'Same as __or__(), but in-place.  O(n)'
        return self.__iset_op(other, method=int.__gt__)

    def __iand__(self, other: collections.Counter) -> collections.Counter:
        'Same as __and__(), but in-place.  O(n)'
        return self.__iset_op(other, method=int.__lt__)
