#-----------------------------------------------------------------------------#
#   set.py                                                                    #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections.abc

from .base import Base
from .base import Iterable
from .base import Pipelined
from .exceptions import KeyExistsError



class RedisSet(Iterable, Pipelined, Base, collections.abc.MutableSet):
    """Redis-backed container compatible with Python sets."""

    def __init__(self, iterable=tuple(), *, redis=None, key=None):
        """Initialize a RedisSet.  O(n)"""
        super().__init__(iterable, redis=redis, key=key)
        self._populate(iterable)

    @Pipelined._watch()
    def _populate(self, iterable=tuple()):
        values = [self._encode(value) for value in iterable]
        if values:
            if self.redis.exists(self.key):
                raise KeyExistsError(self.redis, self.key)
            self.redis.multi()
            self.redis.sadd(self.key, *values)

    def __contains__(self, value):
        """s.__contains__(element) <==> element in s.  O(1)"""
        return self.redis.sismember(self.key, self._encode(value))

    def _scan(self, key, *, cursor=0):
        return self.redis.sscan(key, cursor=cursor)

    def __len__(self):
        """Return the number of elements in a RedisSet.  O(1)"""
        return self.redis.scard(self.key)

    def add(self, value):
        """Add an element to a RedisSet.  O(1)

        This has no effect if the element is already present.
        """
        self.redis.sadd(self.key, self._encode(value))

    def discard(self, value):
        """Remove an element from a RedisSet.  O(1)

        This has no effect if the element is not present.
        """
        self.redis.srem(self.key, self._encode(value))

    def __repr__(self):
        """Return the string representation of a RedisSet.  O(n)"""
        s = self.redis.smembers(self.key)
        s = (self._decode(value) for value in s)
        s = list(str(tuple(s)))
        if s[-2] == ',':
            del s[-2]
        s = ''.join(s)
        return self.__class__.__name__ + s

    def clear(self):
        """Remove the elements in a RedisSet.  O(n)"""
        self.redis.delete(self.key)
