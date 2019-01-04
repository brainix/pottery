#-----------------------------------------------------------------------------#
#   dict.py                                                                   #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections.abc
import contextlib
import itertools

from .base import Base
from .base import Iterable
from .exceptions import KeyExistsError



class RedisDict(Base, Iterable, collections.abc.MutableMapping):
    'Redis-backed container compatible with Python dicts.'

    def __init__(self, iterable=tuple(), *, redis=None, key=None, **kwargs):
        'Initialize a RedisDict.  O(n)'
        super().__init__(redis=redis, key=key, **kwargs)
        with self._watch(iterable):
            if iterable or kwargs:
                if self.redis.exists(self.key):
                    raise KeyExistsError(self.redis, self.key)
                else:
                    self._populate(iterable, **kwargs)

    def _populate(self, iterable=tuple(), **kwargs):
        to_set = {}
        with contextlib.suppress(AttributeError):
            iterable = iterable.items()
        for key, value in itertools.chain(iterable, kwargs.items()):
            to_set[self._encode(key)] = self._encode(value)
        if to_set:
            self.redis.multi()
            self.redis.hmset(self.key, to_set)

    # Methods required by collections.abc.MutableMapping:

    def __getitem__(self, key):
        'd.__getitem__(key) <==> d[key].  O(1)'
        encoded_value = self.redis.hget(self.key, self._encode(key))
        if encoded_value is None:
            raise KeyError(key)
        else:
            return self._decode(encoded_value)

    def __setitem__(self, key, value):
        'd.__setitem__(key, value) <==> d[key] = value.  O(1)'
        self.redis.hset(self.key, self._encode(key), self._encode(value))

    def __delitem__(self, key):
        'd.__delitem__(key) <==> del d[key].  O(1)'
        if not self.redis.hdel(self.key, self._encode(key)):
            raise KeyError(key)

    def _scan(self, key, *, cursor=0):
        return self.redis.hscan(key, cursor=cursor)

    def __len__(self):
        'Return the number of items in a RedisDict.  O(1)'
        return self.redis.hlen(self.key)

    # Methods required for Raj's sanity:

    def __repr__(self):
        'Return the string representation of a RedisDict.  O(n)'
        items = self.redis.hgetall(self.key).items()
        dict_ = {self._decode(key): self._decode(value) for key, value in items}
        return self.__class__.__name__ + str(dict_)

    # Method overrides:

    # From collections.abc.MutableMapping:
    def update(self, iterable=tuple(), **kwargs):
        with self._watch(iterable):
            self._populate(iterable, **kwargs)

    # From collections.abc.Mapping:
    def __contains__(self, key):
        'd.__contains__(key) <==> key in d.  O(1)'
        return bool(self.redis.hexists(self.key, self._encode(key)))
