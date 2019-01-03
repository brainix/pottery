#-----------------------------------------------------------------------------#
#   set.py                                                                    #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections.abc
import itertools

from .base import Base
from .base import Iterable
from .exceptions import KeyExistsError



class RedisSet(Base, Iterable, collections.abc.MutableSet):
    'Redis-backed container compatible with Python sets.'

    def __init__(self, iterable=tuple(), *, redis=None, key=None):
        'Initialize a RedisSet.  O(n)'
        super().__init__(iterable, redis=redis, key=key)
        with self._watch(iterable):
            self._populate(iterable)

    def _populate(self, iterable=tuple()):
        encoded_values = {self._encode(value) for value in iterable}
        if encoded_values:
            if self.redis.exists(self.key):
                raise KeyExistsError(self.redis, self.key)
            else:
                self.redis.multi()
                self.redis.sadd(self.key, *encoded_values)

    # Methods required by collections.abc.MutableSet:

    def __contains__(self, value):
        's.__contains__(element) <==> element in s.  O(1)'
        return self.redis.sismember(self.key, self._encode(value))

    def _scan(self, key, *, cursor=0):
        return self.redis.sscan(key, cursor=cursor)

    def __len__(self):
        'Return the number of elements in a RedisSet.  O(1)'
        return self.redis.scard(self.key)

    def add(self, value):
        'Add an element to a RedisSet.  O(1)'
        self.redis.sadd(self.key, self._encode(value))

    def discard(self, value):
        'Remove an element from a RedisSet.  O(1)'
        self.redis.srem(self.key, self._encode(value))

    # Methods required for Raj's sanity:

    def __repr__(self):
        'Return the string representation of a RedisSet.  O(n)'
        set_ = {self._decode(value) for value in self.redis.smembers(self.key)}
        return self.__class__.__name__ + str(set_)

    # Method overrides:

    # From collections.abc.MutableSet:
    def pop(self):
        'Remove and return an element from a RedisSet().  O(1)'
        encoded_value = self.redis.spop(self.key)
        if encoded_value is None:
            raise KeyError('pop from an empty set')
        else:
            return self._decode(encoded_value)

    # From collections.abc.MutableSet:
    def remove(self, value):
        'Remove an element from a RedisSet().  O(1)'
        if not self.redis.srem(self.key, self._encode(value)):
            raise KeyError(value)

    # From collections.abc.Set:
    def isdisjoint(self, other):
        'Return True if two sets have a null intersection.  O(n)'
        with self._watch(other):
            if isinstance(other, self.__class__) and self.redis == other.redis:
                self.redis.multi()
                self.redis.sinter(self.key, other.key)
                disjoint = not self.redis.execute()[0]
            else:
                disjoint = super().isdisjoint(other)
        return disjoint

    # Where does this method come from?
    def issubset(self, other):                      # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def issuperset(self, other):                    # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def union(self, *args):                         # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def intersection(self, *args):                  # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def difference(self, *args):                    # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def symmetric_difference(self, other):          # pragma: no cover
        raise NotImplementedError

    def _update(self, *iterables, redis_method):
        iterables = tuple(iterables)
        with self._watch(*iterables):
            encoded_values = set()
            for value in itertools.chain(*iterables):
                encoded_values.add(self._encode(value))
            if encoded_values:
                self.redis.multi()
                getattr(self.redis, redis_method)(self.key, *encoded_values)

    # Where does this method come from?
    def update(self, *iterables):
        self._update(*iterables, redis_method='sadd')

    # Where does this method come from?
    def intersection_update(self, *args):           # pragma: no cover
        raise NotImplementedError

    # Where does this method come from?
    def difference_update(self, *iterables):
        self._update(*iterables, redis_method='srem')

    # Where does this method come from?
    def symmetric_difference_update(self, other):   # pragma: no cover
        raise NotImplementedError
