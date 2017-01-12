#-----------------------------------------------------------------------------#
#   deque.py                                                                  #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections
import itertools

from .base import Pipelined
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

    def append(self, element):
        'Add an element to the right side of the RedisDeque.'
        value = self._encode(element)
        self.redis.rpush(self.key, value)

    def appendleft(self, element):
        'Add an element to the left side of the RedisDeque.'
        value = self._encode(element)
        self.redis.lpush(self.key, value)

    def pop(self):
        value = self.redis.rpop(self.key)
        if value is None:
            raise IndexError('pop from an empty {}'.format(self.__class__.__name__))
        else:
            element = self._decode(value)
            return element

    def popleft(self):
        value = self.redis.lpop(self.key)
        if value is None:
            raise IndexError('pop from an empty {}'.format(self.__class__.__name__))
        else:
            element = self._decode(value)
            return element

    @Pipelined._watch_method
    def rotate(self, n=1):
        'Rotate the RedisDeque n steps to the right (default n=1).  If n is negative, rotates left.'
        if n:
            push_method = 'lpush' if n > 0 else 'rpush'
            elements = self[-n:] if n > 0 else self[:-n]
            values = [self._encode(element) for element in elements]
            trim_indices = (0, len(self)-n) if n > 0 else (-n, len(self))

            self.redis.multi()
            getattr(self.redis, push_method)(self.key, *values)
            self.redis.ltrim(self.key, *trim_indices)
