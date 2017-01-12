#-----------------------------------------------------------------------------#
#   list.py                                                                   #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
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
                return func(*args, **kwargs)
            except ResponseError:
                raise IndexError('list assignment index out of range')
        return wrap

    def _slice_to_indices(self, slice_or_index):
        try:
            start = slice_or_index.start or 0
            stop = slice_or_index.stop or len(self)
            step = slice_or_index.step or 1
            indices = range(start, stop, step)
        except AttributeError:
            indices = (slice_or_index,)
        return indices

    def __init__(self, iterable=tuple(), *, redis=None, key=None):
        'Initialize a RedisList.  O(1)'
        super().__init__(iterable, redis=redis, key=key)
        self._populate(iterable)

    @Pipelined._watch_method
    def _populate(self, iterable=tuple()):
        encoded_values = [self._encode(value) for value in iterable]
        if encoded_values:
            if self.redis.exists(self.key):
                raise KeyExistsError(self.redis, self.key)
            else:
                self.redis.multi()
                self.redis.rpush(self.key, *encoded_values)

    # Methods required by collections.abc.MutableSequence:

    def __getitem__(self, index):
        'l.__getitem__(index) <==> l[index].  O(n)'
        try:
            encoded = self.redis.lindex(self.key, index)
            if encoded is None:
                raise IndexError('list index out of range')
            else:
                value = self._decode(encoded)
        except ResponseError:
            # This is monumentally stupid.  Python's list API requires us to
            # get elements by slice (defined as a start index, a stop index,
            # and a step).  Of course, Redis allows us to only get elements by
            # start and stop (no step), because it's Redis.  So our ridiculous
            # hack is to get all of the elements between start and stop from
            # Redis, then discard the ones between step in Python.  More info:
            # http://redis.io/commands/lrange
            indices = self._slice_to_indices(index)
            encoded = self.redis.lrange(self.key, indices[0], indices[-1])
            encoded = encoded[::index.step]
            value = [self._decode(value) for value in encoded]
        return value

    @_raise_on_error
    def __setitem__(self, index, value):
        'l.__setitem__(index, value) <==> l[index] = value.  O(n)'
        try:
            self.redis.lset(self.key, index, self._encode(value))
        except ResponseError:
            with self._watch_context():
                indices, values = self._slice_to_indices(index), value
                encoded_values = [self._encode(value) for value in values]
                for index, encoded_value in zip(indices, encoded_values):
                    self.redis.lset(self.key, index, encoded_value)
                indices, num = indices[len(encoded_values):], 0
                for index in indices:
                    self.redis.lset(self.key, index, None)
                    num += 1
                if num:
                    self.redis.lrem(self.key, None, num=num)

    @_raise_on_error
    def __delitem__(self, index):
        'l.__delitem__(index) <==> del l[index].  O(n)'
        # This is monumentally stupid.  Python's list API requires us to
        # delete an element by *index.*  Of course, Redis doesn't support
        # that, because it's Redis.  Instead, Redis supports deleting an
        # element by *value.*  So our ridiculous hack is to set l[index] to
        # None, then to delete the value None.  More info:
        # http://redis.io/commands/lrem
        with self._watch_context():
            indices, num = self._slice_to_indices(index), 0
            for index in indices:
                self.redis.lset(self.key, index, None)
                num += 1
            if num: # pragma: no cover
                self.redis.lrem(self.key, None, num=num)

    def __len__(self):
        'Return the number of items in a RedisList.  O(1)'
        return self.redis.llen(self.key)

    @Pipelined._watch_method
    def insert(self, index, value):
        'Insert an element into a RedisList before the given index.  O(n)'
        encoded_value = self._encode(value)
        if index <= 0:
            self.redis.multi()
            self.redis.lpush(self.key, encoded_value)
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
            for encoded_value in (encoded_value, pivot):
                self.redis.linsert(self.key, 'BEFORE', None, encoded_value)
            self.redis.lrem(self.key, None, num=1)
        else:
            self.redis.multi()
            self.redis.rpush(self.key, encoded_value)

    def extend(self, values):
        'Extend a Redis by appending elements from the iterable.  O(1)'
        encoded_values = (self._encode(value) for value in values)
        self.redis.rpush(self.key, *encoded_values)

    # Methods required for Raj's sanity:

    def sort(self, *, key=None, reverse=False):
        'Sort a RedisList in place.  O(n)'
        if key is None:
            self.redis.sort(self.key, desc=reverse, store=self.key)
        else:
            raise NotImplementedError('sorting by key not implemented')

    def __eq__(self, other):
        if super().__eq__(other):
            # self and other are both RedisLists on the same Redis instance and
            # with the same key.  No need to compare element by element.
            return True
        else:
            # At the least, self is a RedisList.  other may or may not be a
            # Pottery Redis container.  Watch self's Redis key (and other's
            # Redis key too, if applicable) so that we can do the rest of the
            # equality comparison unperturbed.
            keys_to_watch = [self.key]
            if isinstance(other, Base):
                keys_to_watch.append(other.key)

            with self._watch_context(*keys_to_watch):
                try:
                    if len(self) != len(other):
                        return False
                    elif isinstance(other, collections.abc.Sequence) and \
                         len(self) == len(other) == 0:
                        return True
                    elif self[0] != other[0]:
                        return False
                    else:
                        return self[1:] == other[1:]
                except TypeError:
                    return False

    def __add__(self, other):
        'Append the items in other to a RedisList.  O(1)'
        iterable = itertools.chain(self, other)
        return self.__class__(iterable, redis=self.redis)

    def __repr__(self):
        'Return the string representation of a RedisList.  O(n)'
        encoded = self.redis.lrange(self.key, 0, -1)
        values = [self._decode(value) for value in encoded]
        return self.__class__.__name__ + str(values)
