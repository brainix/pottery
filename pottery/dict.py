#-----------------------------------------------------------------------------#
#   dict.py                                                                   #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections.abc

from .base import Iterable
from .exceptions import KeyExistsError



class RedisDict(Iterable, collections.abc.MutableMapping):
    """Redis-backed container compatible with Python dicts."""

    def __init__(self, *, redis=None, key=None, **kwargs):
        """Initialize a RedisDict.  O(n)"""
        super().__init__(redis=redis, key=key, **kwargs)
        if kwargs:
            if self.redis.exists(self.key):
                raise KeyExistsError(self.redis, self.key)
            with self._pipeline() as pipeline:
                for key, value in kwargs.items():
                    key, value = self._encode(key), self._encode(value)
                    pipeline.hset(self.key, key, value)

    def __getitem__(self, key):
        """d.__getitem__(key) <==> d[key].  O(1)"""
        value = self.redis.hget(self.key, self._encode(key))
        if value is None:
            raise KeyError(key)
        return self._decode(value)

    def __setitem__(self, key, value):
        """d.__setitem__(key, value) <==> d[key] = value.  O(1)"""
        self.redis.hset(self.key, self._encode(key), self._encode(value))

    def __delitem__(self, key):
        """d.__delitem__(key) <==> del d[key].  O(1)"""
        success = self.redis.hdel(self.key, self._encode(key))
        if not bool(success):
            raise KeyError(key)

    def _scan(self, key, *, cursor=0):
        return self.redis.hscan(key, cursor=cursor)

    def __len__(self):
        """Return the number of items in a RedisDict.  O(1)"""
        return self.redis.hlen(self.key)

    def __repr__(self):
        """Return the string representation of a RedisDict.  O(n)"""
        d = self.redis.hgetall(self.key).items()
        d = {self._decode(key): self._decode(value) for key, value in d}
        return self.__class__.__name__ + str(d)
