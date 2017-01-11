#-----------------------------------------------------------------------------#
#   deque.py                                                                  #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections
import itertools

from .list import RedisList



class RedisDeque(RedisList, collections.deque):
    'Redis-backed container compatible with collections.deque.'

    # Method overrides:

    def __init__(self, iterable=tuple(), maxlen=None, *, redis=None, key=None):
        iterable = itertools.islice(iterable, maxlen)
        super().__init__(iterable, redis=redis, key=key)
        self._maxlen = maxlen

    @property
    def maxlen(self):
        return self._maxlen

    def append(self, value):
        'Add an element to the right side of the RedisDeque.'
        value = self._encode(value)
        self.redis.rpush(self.key, value)

    def appendleft(self, value):
        'Add an element to the left side of the RedisDeque.'
        value = self._encode(value)
        self.redis.lpush(self.key, value)

    def pop(self):
        value = self.redis.rpop(self.key)
        decoded = self._decode(value)
        return decoded

    def popleft(self):
        value = self.redis.lpop(self.key)
        decoded = self._decode(value)
        return decoded
