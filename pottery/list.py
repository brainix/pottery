#-----------------------------------------------------------------------------#
#   list.py                                                                   #
#                                                                             #
#   Copyright (c) 2015-2016, Rajiv Bakulesh Shah.                             #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections.abc
import functools
import itertools

from redis import ResponseError

from .base import Base
from .base import Pipelined
from .exceptions import KeyExistsError



class RedisList(Base, collections.abc.MutableSequence):
    'Redis-backed container compatible with Python lists.'

    def _raise_on_error(func):
        @functools.wraps(func)
        def wrap(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except ResponseError:
                raise IndexError('list assignment index out of range')
        return wrap

    def _slice_to_indices(self, slice_or_index):
        try:
            start = 0 if slice_or_index.start is None else slice_or_index.start
            stop = len(self) if slice_or_index.stop is None else slice_or_index.stop
            step = 1 if slice_or_index.step is None else slice_or_index.step
            indices = range(start, stop, step)
        except AttributeError:
            indices = (slice_or_index,)
        return indices

    def __init__(self, iterable=tuple(), *, redis=None, key=None):
        'Initialize a RedisList.  O(1)'
        super().__init__(iterable, redis=redis, key=key)
        self._populate(iterable)

    @Pipelined._watch
    def _populate(self, iterable=tuple()):
        values = [self._encode(value) for value in iterable]
        if values:
            if self.redis.exists(self.key):
                raise KeyExistsError(self.redis, self.key)
            self.redis.multi()
            self.redis.rpush(self.key, *values)

    # Methods required by collections.abc.MutableSequence:

    def __getitem__(self, index):
        'l.__getitem__(index) <==> l[index].  O(n)'
        try:
            value = self.redis.lindex(self.key, index)
            if value is None:
                raise IndexError('list index out of range')
            return self._decode(value)
        except ResponseError:
            # This is monumentally stupid.  Python's list API requires us to
            # get elements by slice (defined as a start index, a stop index,
            # and a step).  Of course, Redis allows us to only get elements by
            # start and stop (no step), because it's Redis.  So our ridiculous
            # hack is to get all of the elements between start and stop from
            # Redis, then discard the ones between step in Python.  More info:
            # http://redis.io/commands/lrange
            indices = self._slice_to_indices(index)
            values = self.redis.lrange(self.key, indices[0], indices[-1])
            values = values[::index.step]
            return [self._decode(value) for value in values]

    @_raise_on_error
    def __setitem__(self, index, value):
        'l.__setitem__(index, value) <==> l[index] = value.  O(n)'
        try:
            self.redis.lset(self.key, index, self._encode(value))
        except ResponseError:
            with self._pipeline as pipeline:
                indices, values = self._slice_to_indices(index), value
                values = [self._encode(value) for value in values]
                for index, value in zip(indices, values):
                    pipeline.lset(self.key, index, value)
                indices, num = indices[len(values):], 0
                for index in indices:
                    pipeline.lset(self.key, index, None)
                    num += 1
                if num:
                    pipeline.lrem(self.key, None, num=num)

    @_raise_on_error
    def __delitem__(self, index):
        'l.__delitem__(index) <==> del l[index].  O(n)'
        # This is monumentally stupid.  Python's list API requires us to
        # delete an element by *index.*  Of course, Redis doesn't support
        # that, because it's Redis.  Instead, Redis supports deleting an
        # element by *value.*  So our ridiculous hack is to set l[index] to
        # None, then to delete the value None.  More info:
        # http://redis.io/commands/lrem
        with self._pipeline as pipeline:
            indices, num = self._slice_to_indices(index), 0
            for index in indices:
                pipeline.lset(self.key, index, None)
                num += 1
            if num:
                pipeline.lrem(self.key, None, num=num)

    def __len__(self):
        'Return the number of items in a RedisList.  O(1)'
        return self.redis.llen(self.key)

    @Pipelined._watch
    def insert(self, index, value):
        'Insert an element into a RedisList before the given index.  O(n)'
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

    # Methods required for Raj's sanity:

    def sort(self, *, key=None, reverse=False):
        'Sort a RedisList in place.  O(n)'
        if key is not None:
            raise NotImplementedError('sorting by key not implemented')
        self.redis.sort(self.key, desc=reverse, store=self.key)

    def __add__(self, other):
        'Append the items in other to a RedisList.  O(1)'
        iterable = itertools.chain(self, other)
        return self.__class__(iterable, redis=self.redis)

    def __repr__(self):
        'Return the string representation of a RedisList.  O(n)'
        l = self.redis.lrange(self.key, 0, -1)
        l = [self._decode(value) for value in l]
        return self.__class__.__name__ + str(l)
