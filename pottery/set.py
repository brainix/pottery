#-----------------------------------------------------------------------------#
#   set.py                                                                    #
#                                                                             #
#   Copyright (c) 2015-2016, Rajiv Bakulesh Shah.                             #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections.abc

from .base import Base
from .base import Iterable
from .base import Pipelined
from .exceptions import KeyExistsError



class RedisSet(Iterable, Base, collections.abc.MutableSet):
    'Redis-backed container compatible with Python sets.'

    def __init__(self, iterable=tuple(), *, redis=None, key=None):
        'Initialize a RedisSet.  O(n)'
        super().__init__(iterable, redis=redis, key=key)
        self._populate(iterable)

    @Pipelined._watch
    def _populate(self, iterable=tuple()):
        values = {self._encode(value) for value in iterable}
        if values:
            if self.redis.exists(self.key):
                raise KeyExistsError(self.redis, self.key)
            self.redis.multi()
            self.redis.sadd(self.key, *values)

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
        s = self.redis.smembers(self.key)
        s = (self._decode(value) for value in s)
        s = list(str(tuple(s)))
        if s[-2] == ',':
            del s[-2]
        s = ''.join(s)
        return self.__class__.__name__ + s

    # Method overrides:

    # From collections.abc.MutableSet:
    def pop(self):
        'Remove and return an element from a RedisSet().  O(1)'
        value = self.redis.spop(self.key)
        if value is None:
            raise KeyError('pop from an empty set')
        return self._decode(value)

    # From collections.abc.MutableSet:
    def remove(self, value):
        'Remove an element from a RedisSet().  O(1)'
        count = self.redis.srem(self.key, self._encode(value))
        if count is 0:
            raise KeyError(value)

    # From collections.abc.Set:
    def isdisjoint(self, other):
        'Return True if two sets have a null intersection.  O(n)'
        if isinstance(other, self.__class__) and self.redis == other.redis:
            return len(self.redis.sinter(self.key, other.key)) is 0
        return super().isdisjoint(other)

    # Where does this method come from?
    def issubset(self, other):
        raise NotImplementedError

    # Where does this method come from?
    def issuperset(self, other):
        raise NotImplementedError

    # Where does this method come from?
    def union(self, *args):
        raise NotImplementedError

    # Where does this method come from?
    def intersection(self, *args):
        raise NotImplementedError

    # Where does this method come from?
    def difference(self, *args):
        raise NotImplementedError

    # Where does this method come from?
    def symmetric_difference(self, other):
        raise NotImplementedError

    # Where does this method come from?
    def update(self, *args):
        raise NotImplementedError

    # Where does this method come from?
    def intersection_update(self, *args):
        raise NotImplementedError

    # Where does this method come from?
    def difference_update(self, *args):
        raise NotImplementedError

    # Where does this method come from?
    def symmetric_difference_update(self, other):
        raise NotImplementedError
