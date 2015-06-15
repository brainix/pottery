#-----------------------------------------------------------------------------#
#   list.py                                                                   #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections.abc
import functools

from redis import ResponseError

from .base import Base
from .base import Pipelined
from .exceptions import KeyExistsError



class RedisList(Pipelined, Base, collections.abc.MutableSequence):
    """Redis-backed container compatible with Python lists."""

    def raise_on_error(func):
        @functools.wraps(func)
        def wrap(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except ResponseError:
                raise IndexError('list assignment index out of range')
        return wrap

    def __init__(self, iterable=tuple(), *, redis=None, key=None):
        """Initialize a RedisList.  O(1)"""
        super().__init__(iterable, redis=redis, key=key)
        self._populate(iterable)

    @Pipelined._watch()
    def _populate(self, iterable=tuple()):
        values = [self._encode(value) for value in iterable]
        if values:
            if self.redis.exists(self.key):
                raise KeyExistsError(self.redis, self.key)
            self.redis.multi()
            self.redis.rpush(self.key, *values)

    def __getitem__(self, index):
        """l.__getitem__(index) <==> l[index].  O(n)"""
        value = self.redis.lindex(self.key, index)
        if value is None:
            raise IndexError('list index out of range')
        return self._decode(value)

    @raise_on_error
    def __setitem__(self, index, value):
        """l.__setitem__(index, value) <==> l[index] = value.  O(n)"""
        self.redis.lset(self.key, index, self._encode(value))

    @raise_on_error
    def __delitem__(self, index):
        """l.__delitem__(index) <==> del l[index].  O(n)"""
        # This is monumentally stupid.  Python's list API requires us to
        # delete an element by *index.*  Of course, Redis doesn't support
        # that, because it's Redis.  Instead, Redis supports deleting an
        # element by *value.*  So our ridiculous hack is to set l[index] to
        # None, then to delete the value None.  More info:
        # http://redis.io/commands/lrem
        with self._pipeline() as pipeline:
            pipeline.lset(self.key, index, None)
            pipeline.lrem(self.key, None, num=1)

    def __len__(self):
        """Return the number of items in a RedisList.  O(1)"""
        return self.redis.llen(self.key)

    @Pipelined._watch()
    def insert(self, index, value):
        """Insert an element into a RedisList before the given index.  O(n)"""
        value = self._encode(value)
        if index <= 0:
            self.redis.multi()
            self.redis.lpush(self.key, value)
        elif index < len(self):
            # This is monumentally stupid.  Python's list API requires us to
            # insert an element before the given *index.*  Of course, Redis
            # doesn't support that, because it's Redis.  Instead, Redis
            # supports inserting an element before a given (pivot) *value.*  So
            # our ridiculous hack is to set the pivot value to None, then to
            # insert the desired value and the original pivot value before the
            # value None, then to delete the value None.  More info:
            # http://redis.io/commands/linsert
            pivot = self._encode(self[index])
            self.redis.multi()
            self.redis.lset(self.key, index, None)
            for value in (value, pivot):
                self.redis.linsert(self.key, 'BEFORE', None, value)
            self.redis.lrem(self.key, None, num=1)
        else:
            self.redis.multi()
            self.redis.rpush(self.key, value)

    def __repr__(self):
        """Return the string representation of a RedisList.  O(n)"""
        l = self.redis.lrange(self.key, 0, -1)
        l = [self._decode(value) for value in l]
        return self.__class__.__name__ + str(l)
