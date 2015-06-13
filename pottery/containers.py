#-----------------------------------------------------------------------------#
#   containers.py                                                             #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections.abc
import contextlib
import json



class _Base:
    def __init__(self, redis, key, *args, **kwargs):
        self._redis = redis
        self._key = key

    def __iter__(self, scan):
        cursor = 0
        while True:
            cursor, iterable = scan(self._key, cursor=cursor)
            for value in iterable:
                value = value.decode('utf-8')
                with contextlib.suppress(ValueError):
                    value = json.loads(value)
                yield value
            if cursor is 0:
                break



class RedisSet(_Base, collections.abc.MutableSet):
    def __init__(self, redis, key, iterable=tuple()):
        """Initialize a RedisSet.  O(n)"""
        super().__init__(redis, key, iterable)
        values = [json.dumps(value) for value in iterable]
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
        s = set(json.loads(value.decode('utf-8')) for value in s)
        return self.__class__.__name__ + str(s)



class RedisDict(_Base, collections.abc.MutableMapping):
    def __init__(self, redis, key, **kwargs):
        """Initialize a RedisDict.  O(n)"""
        super().__init__(redis, key, **kwargs)
        self.update(**kwargs)

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
        """Return the string representation of a RedisDict."""
        d = self._redis.hgetall(self._key).items()
        d = {k.decode('utf-8'): json.loads(v.decode('utf-8')) for k, v in d}
        return self.__class__.__name__ + str(d)
