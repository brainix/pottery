# --------------------------------------------------------------------------- #
#   counter.py                                                                #
#                                                                             #
#   Copyright © 2015-2025, Rajiv Bakulesh Shah, original author.              #
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


# TODO: Remove the following import after deferred evaluation of annotations
# because the default.
#   1. https://docs.python.org/3/whatsnew/3.7.html#whatsnew37-pep563
#   2. https://www.python.org/dev/peps/pep-0563/
#   3. https://www.python.org/dev/peps/pep-0649/
from __future__ import annotations

import collections
import contextlib
import itertools
from typing import Callable
from typing import Iterable
from typing import List
from typing import Tuple
from typing import Union
from typing import cast

from redis.client import Pipeline
from typing_extensions import Counter

from .annotations import JSONTypes
from .dict import RedisDict


InitIter = Iterable[JSONTypes]
InitArg = Union[InitIter, Counter]


class RedisCounter(RedisDict, collections.Counter):
    'Redis-backed container compatible with collections.Counter.'

    # Method overrides:

    def _populate(self,  # type: ignore
                  pipeline: Pipeline,
                  arg: InitArg = tuple(),
                  *,
                  sign: int = +1,
                  **kwargs: int,
                  ) -> None:
        dict_ = {}
        if isinstance(arg, collections.abc.Mapping):
            items = arg.items()
            for key, value in items:
                dict_[key] = sign * value
        else:
            for key in arg:
                value = dict_.get(key, self[key])
                dict_[key] = value + sign

        for key, value in kwargs.items():
            if dict_.get(key, 0) == 0:
                original = self[key]
            else:  # pragma: no cover
                original = dict_[key]
            dict_[key] = original + sign * value

        dict_ = {key: self[key] + value for key, value in dict_.items()}
        encoded_dict = self._encode_dict(dict_)
        if encoded_dict:
            pipeline.multi()  # Available since Redis 1.2.0
            # Available since Redis 2.0.0:
            pipeline.hset(self.key, mapping=encoded_dict)  # type: ignore

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __populate = _populate

    def update(self, arg: InitArg = tuple(), **kwargs: int) -> None:  # type: ignore
        'Like dict.update() but add counts instead of replacing them.  O(n)'
        with self._watch(arg) as pipeline:
            self.__populate(pipeline, arg, sign=+1, **kwargs)

    def subtract(self, arg: InitArg = tuple(), **kwargs: int) -> None:  # type: ignore
        'Like dict.update() but subtracts counts instead of replacing them.  O(n)'
        with self._watch(arg) as pipeline:
            self.__populate(pipeline, arg, sign=-1, **kwargs)

    def __getitem__(self, key: JSONTypes) -> int:
        'c.__getitem__(key) <==> c.get(key, 0).  O(1)'
        try:
            value = cast(int, super().__getitem__(key))
        except KeyError:
            value = super().__missing__(key)  # type: ignore
        return value

    def __delitem__(self, key: JSONTypes) -> None:  # type: ignore
        'c.__delitem__(key) <==> del c[key].  O(1)'
        with contextlib.suppress(KeyError):
            super().__delitem__(key)

    def __repr__(self) -> str:
        'Return the string representation of the RedisCounter.  O(n)'
        items = self.__most_common()
        pairs = (f"'{key}': {value}" for key, value in items)
        repr_ = ', '.join(pairs)
        return self.__class__.__qualname__ + '{' + repr_ + '}'

    def to_counter(self) -> Counter[JSONTypes]:
        'Convert a RedisCounter into a plain Python collections.Counter.'
        return collections.Counter(self)

    __to_counter = to_counter

    def __math_op(self,
                  other: Counter[JSONTypes],
                  *,
                  method: Callable[[Counter[JSONTypes], Counter[JSONTypes]], Counter[JSONTypes]],
                  ) -> Counter[JSONTypes]:
        with self._watch(other):
            counter = self.__to_counter()
            return method(counter, other)

    def __add__(self, other: Counter[JSONTypes]) -> Counter[JSONTypes]:  # type: ignore
        "Return the addition our counts to other's counts, but keep only counts > 0.  O(n)"
        return self.__math_op(other, method=collections.Counter.__add__)

    def __sub__(self, other: Counter[JSONTypes]) -> Counter[JSONTypes]:
        "Return the subtraction other's counts from our counts, but keep only counts > 0.  O(n)"
        return self.__math_op(other, method=collections.Counter.__sub__)

    def __or__(self, other: Counter[JSONTypes]) -> Counter[JSONTypes]:  # type: ignore
        "Return the max of our counts vs. other's counts (union), but keep only counts > 0.  O(n)"
        return self.__math_op(other, method=collections.Counter.__or__)

    def __and__(self, other: Counter[JSONTypes]) -> Counter[JSONTypes]:
        "Return the min of our counts vs. other's counts (intersection) but keep only counts > 0.  O(n)"
        return self.__math_op(other, method=collections.Counter.__and__)

    def __unary_op(self,
                   *,
                   test_func: Callable[[int], bool],
                   modifier_func: Callable[[int], int],
                   ) -> Counter[JSONTypes]:
        with self._watch():
            counter: Counter[JSONTypes] = collections.Counter()
            for key, value in self.__to_counter().items():
                if test_func(value):
                    counter[key] = modifier_func(value)
            return counter

    def __pos__(self) -> Counter[JSONTypes]:
        'Return our counts > 0.  O(n)'
        return self.__unary_op(
            test_func=lambda x: x > 0,
            modifier_func=lambda x: x,
        )

    def __neg__(self) -> Counter[JSONTypes]:
        'Return the absolute value of our counts < 0.  O(n)'
        return self.__unary_op(
            test_func=lambda x: x < 0,
            modifier_func=lambda x: -x,
        )

    def __imath_op(self,
                   other: Counter[JSONTypes],
                   *,
                   sign: int = +1,
                   ) -> RedisCounter:
        with self._watch(other) as pipeline:
            try:
                other_items = cast(RedisCounter, other).to_counter().items()
            except AttributeError:
                other_items = other.items()
            to_set = {k: self[k] + sign * v for k, v in other_items}
            to_del = {k for k, v in to_set.items() if v <= 0}
            to_del.update(
                k for k, v in self.items() if k not in to_set and v <= 0
            )
            encoded_to_set = {
                self._encode(k): self._encode(v) for k, v in to_set.items() if v
            }
            encoded_to_del = {self._encode(k) for k in to_del}
            if encoded_to_set or encoded_to_del:
                pipeline.multi()  # Available since Redis 1.2.0
                if encoded_to_set:
                    # Available since Redis 2.0.0:
                    pipeline.hset(self.key, mapping=encoded_to_set)  # type: ignore
                if encoded_to_del:
                    pipeline.hdel(self.key, *encoded_to_del)  # Available since Redis 2.0.0
            return self

    def __iadd__(self, other: Counter[JSONTypes]) -> Counter[JSONTypes]:  # type: ignore
        'Same as __add__(), but in-place.  O(n)'
        return self.__imath_op(other, sign=+1)

    def __isub__(self, other: Counter[JSONTypes]) -> Counter[JSONTypes]:  # type: ignore
        'Same as __sub__(), but in-place.  O(n)'
        return self.__imath_op(other, sign=-1)

    def __iset_op(self,
                  other: Counter[JSONTypes],
                  *,
                  method: Callable[[int, int], bool],
                  ) -> RedisCounter:
        with self._watch(other) as pipeline:
            self_counter = self.__to_counter()
            try:
                other_counter = cast(RedisCounter, other).to_counter()
            except AttributeError:
                other_counter = other
            to_set, to_del = {}, set()
            for k in itertools.chain(self_counter, other_counter):
                if method(self_counter[k], other_counter[k]):
                    to_set[k] = self_counter[k]
                else:
                    to_set[k] = other_counter[k]
                if to_set[k] <= 0:
                    del to_set[k]
                    to_del.add(k)
            if to_set or to_del:
                pipeline.multi()  # Available since Redis 1.2.0
            if to_set:
                encoded_to_set = {
                    self._encode(k): self._encode(v) for k, v in to_set.items()
                }
                # Available since Redis 2.0.0:
                pipeline.hset(self.key, mapping=encoded_to_set)  # type: ignore
            if to_del:
                encoded_to_del = {self._encode(k) for k in to_del}
                pipeline.hdel(self.key, *encoded_to_del)  # Available since Redis 2.0.0
        return self

    def __ior__(self, other: Counter[JSONTypes]) -> Counter[JSONTypes]:  # type: ignore
        'Same as __or__(), but in-place.  O(n)'
        return self.__iset_op(other, method=int.__gt__)

    def __iand__(self, other: Counter[JSONTypes]) -> Counter[JSONTypes]:  # type: ignore
        'Same as __and__(), but in-place.  O(n)'
        return self.__iset_op(other, method=int.__lt__)

    def most_common(self,
                    n: int | None = None,
                    ) -> List[Tuple[JSONTypes, int]]:
        counter = self.__to_counter()
        return counter.most_common(n=n)

    __most_common = most_common
