#-----------------------------------------------------------------------------#
#   deque.py                                                                  #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections

from .list import RedisList



class RedisDeque(RedisList, collections.deque):
    'Redis-backed container compatible with collections.deque.'

    # Method overrides:

    def __init__(self, iterable=tuple(), maxlen=None, *, redis=None, key=None):
        'Initialize a RedisDeque.  O(n)'
        self._maxlen = maxlen
        super().__init__(iterable, redis=redis, key=key)

    def _populate(self, iterable=tuple()):
        if self.maxlen is not None:
            try:
                if self.maxlen:
                    iterable = tuple(iterable)[-self.maxlen:]
                else:
                    iterable = tuple()
            except TypeError:
                raise TypeError('an integer is required')
        super()._populate(iterable)
        if not iterable and self.maxlen is not None and len(self) > self.maxlen:
            raise IndexError('persistent {} beyond its maximum size'.format(self.__class__.__name__))

    @property
    def maxlen(self):
        return self._maxlen

    @maxlen.setter
    def maxlen(self, value):
        raise AttributeError("attribute 'maxlen' of '{}' objects is not writable".format(self.__class__.__name__))

    def insert(self, index, value):
        'Insert an element into a RedisDeque before the given index.  O(n)'
        with self._watch_context():
            if self.maxlen is not None and len(self) >= self.maxlen:
                raise IndexError('{} already at its maximum size'.format(self.__class__.__name__))
            else:
                return super()._insert(index, value)

    def append(self, value):
        'Add an element to the right side of the RedisDeque.  O(1)'
        with self._watch_context():
            len_ = len(self) + 1
            self.redis.multi()
            self.redis.rpush(self.key, self._encode(value))
            if self.maxlen is not None and len_ >= self.maxlen:
                self.redis.ltrim(self.key, len_-self.maxlen, len_)

    def appendleft(self, value):
        'Add an element to the left side of the RedisDeque.  O(1)'
        with self._watch_context():
            len_ = len(self) + 1
            self.redis.multi()
            self.redis.lpush(self.key, self._encode(value))
            if self.maxlen is not None and len_ >= self.maxlen:
                self.redis.ltrim(self.key, 0, self.maxlen-1)

    def extend(self, values):
        'Extend a RedisList by appending elements from the iterable.  O(1)'
        with self._watch_context():
            encoded_values = [self._encode(value) for value in values]
            len_ = len(self) + len(encoded_values)
            self.redis.multi()
            self.redis.rpush(self.key, *encoded_values)
            if self.maxlen is not None and len_ >= self.maxlen:
                self.redis.ltrim(self.key, len_-self.maxlen, len_)

    def extendleft(self, values):
        '''Extend a RedisList by prepending elements from the iterable.  O(1)
        
        Note the order in which the elements are prepended from the iterable:

            >>> d = RedisDeque()
            >>> d.extendleft('abc')
            >>> d
            RedisDeque(['c', 'b', 'a'])
        '''
        with self._watch_context():
            encoded_values = [self._encode(value) for value in values]
            len_ = len(self) + len(encoded_values)
            self.redis.multi()
            self.redis.lpush(self.key, *encoded_values)
            if self.maxlen is not None and len_ >= self.maxlen:
                self.redis.ltrim(self.key, 0, self.maxlen-1)

    def pop(self):
        return super().pop()

    def popleft(self):
        encoded_value = self.redis.lpop(self.key)
        if encoded_value is None:
            raise IndexError('pop from an empty {}'.format(self.__class__.__name__))
        else:
            return self._decode(encoded_value)

    def rotate(self, n=1):
        'Rotate the RedisDeque n steps to the right (default n=1).  If n is negative, rotates left.'
        if n:
            with self._watch_context():
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
            repr += ', ' + 'maxlen={}'.format(self.maxlen)
        repr += ')'
        return repr
