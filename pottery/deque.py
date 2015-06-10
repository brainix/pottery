#-----------------------------------------------------------------------------#
#   deque.py                                                                  #
#                                                                             #
#   Copyright (c) 2015-2016, Rajiv Bakulesh Shah.                             #
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

    def insert(*args, **kwargs):
        raise NotImplementedError
