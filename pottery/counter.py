#-----------------------------------------------------------------------------#
#   counter.py                                                                #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections
import contextlib
import itertools

from .dict import RedisDict



class RedisCounter(RedisDict, collections.Counter):
    'Redis-backed container compatible with collections.Counter.'

    # Method overrides:

    def _populate(self, iterable=tuple(), *, sign=+1, **kwargs):
        to_set = {}
        try:
            for key, value in iterable.items():
                to_set[key] = sign * value
        except AttributeError:
            for key in iterable:
                to_set[key] = to_set.get(key, self[key]) + sign
        for key, value in kwargs.items():
            original = self[key] if to_set.get(key, 0) == 0 else to_set[key]
            to_set[key] = original + sign * value
        to_set = {key: self[key] + value for key, value in to_set.items()}
        to_set = {
            self._encode(k): self._encode(v) for k, v in to_set.items()
        }
        if to_set:
            self.redis.multi()
            self.redis.hmset(self.key, to_set)

    def update(self, iterable=tuple(), **kwargs):
        'Like dict.update() but add counts instead of replacing them.  O(n)'
        with self._watch(iterable):
            self._populate(iterable, sign=+1, **kwargs)

    def subtract(self, iterable=tuple(), **kwargs):
        'Like dict.update() but subtracts counts instead of replacing them.  O(n)'
        with self._watch(iterable):
            self._populate(iterable, sign=-1, **kwargs)

    def __getitem__(self, key):
        'c.__getitem__(key) <==> c.get(key, 0).  O(1)'
        try:
            value = super().__getitem__(key)
        except KeyError:
            value = super().__missing__(key)
        return value

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
        with self._watch(other):
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

    def _unary_op(self, *, test_func, modifier_func):
        counter = collections.Counter()
        for key, value in self.items():
            if test_func(value):
                counter[key] = modifier_func(value)
        return counter

    def __pos__(self):
        'Return our counts > 0.  O(n)'
        return self._unary_op(
            test_func=lambda x: x > 0,
            modifier_func=lambda x: x,
        )

    def __neg__(self):
        'Return the absolute value of our counts < 0.  O(n)'
        return self._unary_op(
            test_func=lambda x: x < 0,
            modifier_func=lambda x: -x,
        )

    def _imath_op(self, other, *, sign=+1):
        with self._watch(other):
            to_set = {k: self[k] + sign * v for k, v in other.items()}
            to_del = {k for k, v in to_set.items() if v <= 0}
            to_del.update(
                k for k, v in self.items() if k not in to_set and v <= 0
            )
            to_set = {
                self._encode(k): self._encode(v) for k, v in to_set.items() if v
            }
            to_del = {self._encode(k) for k in to_del}
            if to_set or to_del:
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

    def _iset_op(self, other, *, func):
        with self._watch(other):
            to_set, to_del = {}, set()
            for k in itertools.chain(self, other):
                if getattr(self[k], func)(other[k]):
                    to_set[k] = self[k]
                else:
                    to_set[k] = other[k]
                if to_set[k] <= 0:
                    del to_set[k]
                    to_del.add(k)
            if to_set or to_del:
                self.redis.multi()
                if to_set:
                    to_set = {
                        self._encode(k): self._encode(v)
                        for k, v in to_set.items()
                    }
                    self.redis.hmset(self.key, to_set)
                if to_del:
                    to_del = {self._encode(k) for k in to_del}
                    self.redis.hdel(self.key, *to_del)
        return self

    def __ior__(self, other):
        'Same as __or__(), but in-place.  O(n)'
        return self._iset_op(other, func='__gt__')

    def __iand__(self, other):
        'Same as __and__(), but in-place.  O(n)'
        return self._iset_op(other, func='__lt__')
