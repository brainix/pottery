#-----------------------------------------------------------------------------#
#   counter.py                                                                #
#                                                                             #
#   Copyright (c) 2015-2016, Rajiv Bakulesh Shah.                             #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections
import contextlib
import itertools

from .base import Pipelined
from .dict import RedisDict



class RedisCounter(RedisDict, collections.Counter):
    'Redis-backed container compatible with collections.Counter.'

    # Method overrides:

    @Pipelined._watch
    def _update(self, iterable=tuple(), *, sign=+1, **kwargs):
        to_set = {}
        try:
            for key, value in iterable.items():
                to_set[key] = sign * value
        except AttributeError:
            for key in iterable:
                to_set[key] = to_set.get(key, self[key]) + sign
        for key, value in kwargs.items():
            original = self[key] if to_set.get(key, 0) is 0 else to_set[key]
            to_set[key] = original + sign * value
        to_set = {key: self[key] + value for key, value in to_set.items()}
        to_set = {self._encode(k): self._encode(v) for k, v in to_set.items()}
        self.redis.multi()
        if to_set:
            self.redis.hmset(self.key, to_set)

    def update(self, iterable=tuple(), **kwargs):
        'Like dict.update() but add counts instead of replacing them.  O(n)'
        self._update(iterable, sign=+1, **kwargs)

    def subtract(self, iterable=tuple(), **kwargs):
        'Like dict.update() but subtracts counts instead of replacing them.  O(n)'
        self._update(iterable, sign=-1, **kwargs)

    def __getitem__(self, key):
        'c.__getitem__(key) <==> c.get(key, 0).  O(1)'
        with contextlib.suppress(KeyError):
            return super().__getitem__(key)
        return super().__missing__(key)

    def __delitem__(self, key):
        'c.__delitem__(key) <==> del c[key].  O(1)'
        with contextlib.suppress(KeyError):
            super().__delitem__(key)

    def __repr__(self):
        'Return the string representation of a RedisCounter.  O(n)'
        items = self.most_common()
        items = ("'{}': {}".format(key, value) for key, value in items)
        items = ', '.join(items)
        return self.__class__.__name__ + '{' + items + '}'

    def _math_op(self, other, *, func):
        counter = collections.Counter(self.elements())
        return func(counter, other)

    def __add__(self, other):
        "Return the addition our counts to other's counts, but keep only counts > 0.  O(n)"
        return self._math_op(other, func=collections.Counter.__add__)

    def __sub__(self, other):
        "Return the subtraction other's counts from our counts, but keep only counts > 0.  O(n)"
        return self._math_op(other, func=collections.Counter.__sub__)

    def __or__(self, other):
        "Return the max of our counts vs. other's counts (union), but keep only counts > 0.  O(n)"
        return self._math_op(other, func=collections.Counter.__or__)

    def __and__(self, other):
        "Return the min of our counts vs. other's counts (intersection) but keep only counts > 0.  O(n)"
        return self._math_op(other, func=collections.Counter.__and__)

    def __pos__(self, other):
        'Return our counts > 0.  O(n)'
        return self._math_op(other, func=collections.Counter.__pos__)

    def __neg__(self, other):
        'Return the absolute value of our counts < 0.  O(n)'
        return self._math_op(other, func=collections.Counter.__neg__)

    @Pipelined._watch
    def _imath_op(self, other, *, sign=+1):
        to_set = {k: self[k] + sign * v for k, v in other.items()}
        to_del = [k for k, v in to_set.items() if v <= 0]
        to_del.extend([k for k, v in self.items() if k not in to_set and v <= 0])
        to_set = {self._encode(k): self._encode(v) for k, v in to_set.items()}
        to_del = [self._encode(k) for k in to_del]
        self.redis.multi()
        if to_set:
            self.redis.hmset(self.key, to_set)
        if to_del:
            self.redis.hdel(self.key, *to_del)
        return self

    def __iadd__(self, other):
        'Same as __add__(), but in-place.  O(n)'
        return self._imath_op(other, sign=+1)

    def __isub__(self, other):
        'Same as __sub__(), but in-place.  O(n)'
        return self._imath_op(other, sign=-1)

    @Pipelined._watch
    def _iset_op(self, other, *, func):
        to_set, to_del = {}, []
        for k in itertools.chain(self, other):
            to_set[k] = self[v] if getattr(self[v], func)(other[v]) else other[v]
            if to_set[k] <= 0:
                to_del.append(k)
        self.redis.multi()
        if to_set:
            self.redis.hmset(self.key, to_set)
        if to_del:
            self.redis.hdel(self.key, *to_del)
        return self

    def __ior__(self, other):
        'Same as __or__(), but in-place.  O(n)'
        return self._iset_op(other, func='__gt__')

    def __iand__(self, other):
        'Same as __and__(), but in-place.  O(n)'
        return self._iset_op(other, func='__lt__')
