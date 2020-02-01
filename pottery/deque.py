# --------------------------------------------------------------------------- #
#   deque.py                                                                  #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import collections

from .list import RedisList


class RedisDeque(RedisList, collections.deque):
    'Redis-backed container compatible with collections.deque.'

    # Method overrides:

    def __init__(self, iterable=tuple(), maxlen=None, *, redis=None, key=None):
        'Initialize a RedisDeque.  O(n)'
        if maxlen is not None and not isinstance(maxlen, int):
            raise TypeError('an integer is required')
        self._maxlen = maxlen
        super().__init__(iterable, redis=redis, key=key)
        if not iterable and self.maxlen is not None and len(self) > self.maxlen:
            raise IndexError(
                'persistent {} beyond its maximum size'.format(
                    self.__class__.__name__,
                ),
            )

    def _populate(self, iterable=tuple()):
        if self.maxlen is not None:
            if self.maxlen:
                iterable = tuple(iterable)[-self.maxlen:]
            else:  # pragma: no cover
                iterable = tuple()
        super()._populate(iterable)

    @property
    def maxlen(self):
        return self._maxlen

    @maxlen.setter
    def maxlen(self, value):
        raise AttributeError(
            "attribute 'maxlen' of '{}' objects is not writable".format(
                self.__class__.__name__,
            ),
        )

    def insert(self, index, value):
        'Insert an element into a RedisDeque before the given index.  O(n)'
        with self._watch():
            if self.maxlen is not None and len(self) >= self.maxlen:
                raise IndexError(
                    '{} already at its maximum size'.format(
                        self.__class__.__name__,
                    ),
                )
            else:
                return super()._insert(index, value)

    def append(self, value):
        'Add an element to the right side of the RedisDeque.  O(1)'
        self._extend((value,), right=True)

    def appendleft(self, value):
        'Add an element to the left side of the RedisDeque.  O(1)'
        self._extend((value,), right=False)

    def extend(self, values):
        'Extend a RedisList by appending elements from the iterable.  O(1)'
        self._extend(values, right=True)

    def extendleft(self, values):
        '''Extend a RedisList by prepending elements from the iterable.  O(1)

        Note the order in which the elements are prepended from the iterable:

            >>> d = RedisDeque()
            >>> d.extendleft('abc')
            >>> d
            RedisDeque(['c', 'b', 'a'])
        '''
        self._extend(values, right=False)

    def _extend(self, values, *, right=True):
        with self._watch(values):
            encoded_values = [self._encode(value) for value in values]
            len_ = len(self) + len(encoded_values)
            self.redis.multi()
            push_method = 'rpush' if right else 'lpush'
            getattr(self.redis, push_method)(self.key, *encoded_values)
            if self.maxlen is not None and len_ >= self.maxlen:
                if right:
                    trim_indices = len_-self.maxlen, len_
                else:
                    trim_indices = 0, self.maxlen-1
                self.redis.ltrim(self.key, *trim_indices)

    def pop(self):
        return super().pop()

    def popleft(self):
        return super().pop(0)

    def rotate(self, n=1):
        '''Rotate the RedisDeque n steps to the right (default n=1).

        If n is negative, rotates left.
        '''
        if n:
            with self._watch():
                push_method = 'lpush' if n > 0 else 'rpush'
                values = self[-n:] if n > 0 else self[:-n]
                encoded_values = (self._encode(element) for element in values)
                trim_indices = (0, len(self)-n) if n > 0 else (-n, len(self))

                self.redis.multi()
                getattr(self.redis, push_method)(self.key, *encoded_values)
                self.redis.ltrim(self.key, *trim_indices)

    # Methods required for Raj's sanity:

    def __repr__(self):
        'Return the string representation of a RedisDeque.  O(n)'
        encoded = self.redis.lrange(self.key, 0, -1)
        values = [self._decode(value) for value in encoded]
        repr = self.__class__.__name__ + '(' + str(values)
        if self.maxlen is not None:
            repr += ', maxlen={}'.format(self.maxlen)
        repr += ')'
        return repr
