# --------------------------------------------------------------------------- #
#   list.py                                                                   #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import collections.abc
import functools
import itertools

from redis import ResponseError

from .base import Base
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
        except AttributeError:
            start = slice_or_index
            stop = slice_or_index + 1
            step = 1
        indices = range(start, stop, step)
        return indices

    def __init__(self, iterable=tuple(), *, redis=None, key=None):
        'Initialize a RedisList.  O(n)'
        super().__init__(iterable, redis=redis, key=key)
        if iterable:
            with self._watch(iterable):
                self._populate(iterable)

    def _populate(self, iterable=tuple()):
        encoded_values = [self._encode(value) for value in iterable]
        if encoded_values:  # pragma: no cover
            if self.redis.exists(self.key):
                raise KeyExistsError(self.redis, self.key)
            else:
                self.redis.multi()
                self.redis.rpush(self.key, *encoded_values)

    # Methods required by collections.abc.MutableSequence:

    def __getitem__(self, index):
        'l.__getitem__(index) <==> l[index].  O(n)'
        with self._watch():
            if isinstance(index, slice):
                # This is monumentally stupid.  Python's list API requires us
                # to get elements by slice (defined as a start index, a stop
                # index, and a step).  Of course, Redis allows us to only get
                # elements by start and stop (no step), because it's Redis.  So
                # our ridiculous hack is to get all of the elements between
                # start and stop from Redis, then discard the ones between step
                # in Python.  More info:
                # http://redis.io/commands/lrange
                indices = self._slice_to_indices(index)
                self.redis.multi()
                self.redis.lrange(self.key, indices[0], indices[-1])
                encoded = self.redis.execute()[0]
                encoded = encoded[::index.step]
                value = [self._decode(value) for value in encoded]
            else:
                self.redis.multi()
                self.redis.lindex(self.key, index)
                encoded = self.redis.execute()[0]
                if encoded is None:
                    raise IndexError('list index out of range')
                else:
                    value = self._decode(encoded)
            return value

    @_raise_on_error
    def __setitem__(self, index, value):
        'l.__setitem__(index, value) <==> l[index] = value.  O(n)'
        with self._watch():
            if isinstance(index, slice):
                encoded_values = [self._encode(value) for value in value]
                indices = self._slice_to_indices(index)
                self.redis.multi()
                for index, encoded_value in zip(indices, encoded_values):
                    self.redis.lset(self.key, index, encoded_value)
                indices, num = indices[len(encoded_values):], 0
                for index in indices:
                    self.redis.lset(self.key, index, 0)
                    num += 1
                if num:
                    self.redis.lrem(self.key, num, 0)
            else:
                self.redis.multi()
                self.redis.lset(self.key, index, self._encode(value))

    @_raise_on_error
    def __delitem__(self, index):
        'l.__delitem__(index) <==> del l[index].  O(n)'
        # This is monumentally stupid.  Python's list API requires us to
        # delete an element by *index.*  Of course, Redis doesn't support
        # that, because it's Redis.  Instead, Redis supports deleting an
        # element by *value.*  So our ridiculous hack is to set l[index] to
        # None, then to delete the value None.  More info:
        # http://redis.io/commands/lrem
        with self._watch():
            self._delete(index)

    def _delete(self, index):
        indices, num = self._slice_to_indices(index), 0
        self.redis.multi()
        for index in indices:
            self.redis.lset(self.key, index, 0)
            num += 1
        if num:  # pragma: no cover
            self.redis.lrem(self.key, num, 0)

    def __len__(self):
        'Return the number of items in a RedisList.  O(1)'
        return self.redis.llen(self.key)

    def insert(self, index, value):
        'Insert an element into a RedisList before the given index.  O(n)'
        with self._watch():
            self._insert(index, value)

    def _insert(self, index, value):
        encoded_value = self._encode(value)
        if index <= 0:
            self.redis.multi()
            self.redis.lpush(self.key, encoded_value)
        elif index < len(self):
            # This is monumentally stupid.  Python's list API requires us
            # to insert an element before the given *index.*  Of course,
            # Redis doesn't support that, because it's Redis.  Instead,
            # Redis supports inserting an element before a given (pivot)
            # *value.*  So our ridiculous hack is to set the pivot value to
            # None, then to insert the desired value and the original pivot
            # value before the value None, then to delete the value None.
            # More info:
            # http://redis.io/commands/linsert
            pivot = self._encode(self[index])
            self.redis.multi()
            self.redis.lset(self.key, index, 0)
            for encoded_value in (encoded_value, pivot):
                self.redis.linsert(self.key, 'BEFORE', 0, encoded_value)
            self.redis.lrem(self.key, 1, 0)
        else:
            self.redis.multi()
            self.redis.rpush(self.key, encoded_value)

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
            with self._watch(other):
                try:
                    if len(self) != len(other):
                        # self and other are different lengths.
                        return False
                    elif isinstance(other, collections.abc.Sequence):
                        # self and other are the same length, and other is an
                        # ordered collection too.  Compare self's and other's
                        # elements, pair by pair.
                        for value1, value2 in zip(self, other):
                            if value1 != value2:
                                return False
                        else:
                            return True
                    else:
                        # self and other are the same length, but other is an
                        # unordered collection.
                        return False
                except TypeError:
                    return False

    def __add__(self, other):
        'Append the items in other to a RedisList.  O(n)'
        with self._watch(other):
            iterable = itertools.chain(self, other)
            return self.__class__(iterable, redis=self.redis)

    def __repr__(self):
        'Return the string representation of a RedisList.  O(n)'
        encoded = self.redis.lrange(self.key, 0, -1)
        values = [self._decode(value) for value in encoded]
        return self.__class__.__name__ + str(values)

    # Method overrides:

    # From collections.abc.MutableSequence:
    def append(self, value):
        'Add an element to the right side of the RedisList.  O(1)'
        self.extend((value,))

    # From collections.abc.MutableSequence:
    def extend(self, values):
        'Extend a RedisList by appending elements from the iterable.  O(1)'
        with self._watch(values):
            encoded_values = (self._encode(value) for value in values)
            self.redis.multi()
            self.redis.rpush(self.key, *encoded_values)

    # From collections.abc.MutableSequence:
    def pop(self, index=None):
        with self._watch():
            len_ = len(self)
            if index and index >= len_:
                raise IndexError('pop index out of range')
            elif index in {0, None, len_-1, -1}:
                pop_method = 'lpop' if index == 0 else 'rpop'
                self.redis.multi()
                getattr(self.redis, pop_method)(self.key)
                encoded_value = self.redis.execute()[0]
                if encoded_value is None:
                    raise IndexError(
                        'pop from an empty {}'.format(self.__class__.__name__),
                    )
                else:
                    return self._decode(encoded_value)
            else:
                value = self[index]
                self._delete(index)
                return value

    # From collections.abc.MutableSequence:
    def remove(self, value):
        with self._watch():
            for index, element in enumerate(self):
                if element == value:
                    self._delete(index)
                    break
            else:
                raise ValueError(
                    '{class_}.remove(x): x not in {class_}'.format(
                        class_=self.__class__.__name__,
                    ),
                )
