#-----------------------------------------------------------------------------#
#   containers.py                                                             #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections.abc
import contextlib
import functools
import json

from redis import Redis
from redis.exceptions import ResponseError

from .exceptions import KeyExistsError



class _Base:
    _DEFAULT_REDIS_URL = 'http://localhost:6379/'

    def __init__(self, redis, key, *args, **kwargs):
        if redis is None:
            redis = Redis.from_url(self._DEFAULT_REDIS_URL)
        self._redis = redis
        self._key = key

    @contextlib.contextmanager
    def _pipeline(self, *, transaction=True, shard_hint=None, raise_on_error=True):
        pipeline = self._redis.pipeline(transaction=transaction, shard_hint=shard_hint)
        try:
            yield pipeline
        finally:
            pipeline.execute(raise_on_error=raise_on_error)



class _Iterable:
    def __iter__(self, scan):
        cursor = 0
        while True:
            cursor, iterable = scan(self._key, cursor=cursor)
            for value in iterable:
                yield value
            if cursor == 0:
                break



class RedisList(_Base, collections.abc.MutableSequence):
    """Redis-backed container compatible with Python lists."""

    def raise_on_error(func):
        @functools.wraps(func)
        def wrap(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except ResponseError:
                raise IndexError('list assignment index out of range')
        return wrap

    def __init__(self, redis, key, iterable=tuple()):
        """Initialize a RedisList.  O(1)"""
        super().__init__(redis, key, iterable)
        values = [json.dumps(value) for value in iterable]
        if values:
            if self._redis.exists(self._key):
                raise KeyExistsError(self._redis, self._key)
            self._redis.rpush(self._key, *values)

    def __getitem__(self, index):
        """l.__getitem__(index) <==> l[index].  O(n)"""
        value = self._redis.lindex(self._key, index)
        if value is None:
            raise IndexError('list index out of range')
        return json.loads(value.decode('utf-8'))

    @raise_on_error
    def __setitem__(self, index, value):
        """l.__setitem__(index, value) <==> l[index] = value.  O(n)"""
        self._redis.lset(self._key, index, json.dumps(value))

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
            pipeline.lset(self._key, index, None)
            pipeline.lrem(self._key, None, num=1)

    def __len__(self):
        """Return the number of items in a RedisList.  O(1)"""
        return self._redis.llen(self._key)

    def insert(self, index, value):
        """Insert an element into a RedisList before the given index.  O(n)"""
        value = json.dumps(value)
        if index <= 0:
            self._redis.lpush(self._key, value)
        elif index < len(self):
            # This is monumentally stupid.  Python's list API requires us to
            # insert an element before the given *index.*  Of course, Redis
            # doesn't support that, because it's Redis.  Instead, Redis
            # supports inserting an element before a given (pivot) *value.*  So
            # our ridiculous hack is to set the pivot value to None, then to
            # insert the desired value and the original pivot value before the
            # value None, then to delete the value None.  More info:
            # http://redis.io/commands/linsert
            pivot = json.dumps(self[index])
            with self._pipeline() as pipeline:
                pipeline.lset(self._key, index, None)
                for value in (value, pivot):
                    pipeline.linsert(self._key, 'BEFORE', None, value)
                pipeline.lrem(self._key, None, num=1)
        else:
            self._redis.rpush(self._key, value)

    def __repr__(self):
        """Return the string representation of a RedisList.  O(n)"""
        l = self._redis.lrange(self._key, 0, -1)
        l = [json.loads(value.decode('utf-8')) for value in l]
        return self.__class__.__name__ + str(l)



class RedisSet(_Iterable, _Base, collections.abc.MutableSet):
    """Redis-backed container compatible with Python sets."""

    def __init__(self, redis, key, iterable=tuple()):
        """Initialize a RedisSet.  O(n)"""
        super().__init__(redis, key, iterable)
        values = [json.dumps(value) for value in iterable]
        if values:
            if self._redis.exists(self._key):
                raise KeyExistsError(self._redis, self._key)
            self._redis.sadd(self._key, *values)

    def __contains__(self, value):
        """s.__contains__(element) <==> element in s.  O(1)"""
        return self._redis.sismember(self._key, json.dumps(value))

    def __iter__(self):
        """Iterate over the elements in a RedisSet.  O(n)"""
        return super().__iter__(self._redis.sscan)

    def __len__(self):
        """Return the number of elements in a RedisSet.  O(1)"""
        return self._redis.scard(self._key)

    def add(self, value):
        """Add an element to a RedisSet.  O(1)

        This has no effect if the element is already present.
        """
        self._redis.sadd(self._key, json.dumps(value))

    def discard(self, value):
        """Remove an element from a RedisSet.  O(1)

        This has no effect if the element is not present.
        """
        self._redis.srem(self._key, json.dumps(value))

    def __repr__(self):
        """Return the string representation of a RedisSet.  O(n)"""
        s = self._redis.smembers(self._key)
        s = (json.loads(value.decode('utf-8')) for value in s)
        s = list(str(tuple(s)))
        if s[-2] == ',':
            del s[-2]
        s = ''.join(s)
        return self.__class__.__name__ + s

    def clear(self):
        """Remove the elements in a RedisSet.  O(n)"""
        self._redis.delete(self._key)



class RedisDict(_Iterable, _Base, collections.abc.MutableMapping):
    """Redis-backed container compatible with Python dicts."""

    def __init__(self, redis, key, **kwargs):
        """Initialize a RedisDict.  O(n)"""
        super().__init__(redis, key, **kwargs)
        if kwargs:
            if self._redis.exists(self._key):
                raise KeyExistsError(self._redis, self._key)
            with self._pipeline() as pipeline:
                for key, value in kwargs.items():
                    pipeline.hset(self._key, key, json.dumps(value))

    def __getitem__(self, key):
        """d.__getitem__(key) <==> d[key].  O(1)"""
        value = self._redis.hget(self._key, key)
        if value is None:
            raise KeyError(key)
        return json.loads(value.decode('utf-8'))

    def __setitem__(self, key, value):
        """d.__setitem__(key, value) <==> d[key] = value.  O(1)"""
        self._redis.hset(self._key, key, json.dumps(value))

    def __delitem__(self, key):
        """d.__delitem__(key) <==> del d[key].  O(1)"""
        success = self._redis.hdel(self._key, key)
        if not bool(success):
            raise KeyError(key)

    def __iter__(self):
        """Iterate over the items in a RedisDict.  O(n)"""
        return super().__iter__(self._redis.hscan)

    def __len__(self):
        """Return the number of items in a RedisDict.  O(1)"""
        return self._redis.hlen(self._key)

    def __repr__(self):
        """Return the string representation of a RedisDict.  O(n)"""
        d = self._redis.hgetall(self._key).items()
        d = {k.decode('utf-8'): json.loads(v.decode('utf-8')) for k, v in d}
        return self.__class__.__name__ + str(d)
