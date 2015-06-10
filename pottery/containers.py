#-----------------------------------------------------------------------------#
#   containers.py                                                             #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections.abc
import contextlib
import json



TMP_KEY = 'tmp'



@contextlib.contextmanager
def _pipeline(redis, *, transaction=True, shard_hint=None, raise_on_error=True):
    pipeline = redis.pipeline(transaction=transaction, shard_hint=shard_hint)
    try:
        yield pipeline
    finally:
        pipeline.execute(raise_on_error=raise_on_error)



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
        super().__init__(redis, key, iterable)
        for value in iterable:
            self._redis.sadd(self._key, json.dumps(value))

    def __contains__(self, value):
        return self._redis.sismember(self._key, json.dumps(value))

    def __iter__(self):
        return super().__iter__(self._redis.sscan)

    def __len__(self):
        return self._redis.scard(self._key)

    def add(self, value):
        self._redis.sadd(self._key, json.dumps(value))

    def discard(self, value):
        with _pipeline(self._redis) as pipeline:
            pipeline.smove(self._key, TMP_KEY, json.dumps(value))
            pipeline.delete(TMP_KEY)



class RedisDict(_Base, collections.abc.MutableMapping):
    def __init__(self, redis, key, **kwargs):
        super().__init__(redis, key, **kwargs)
        self.update(**kwargs)

    def __getitem__(self, key):
        value = self._redis.hget(self._key, key)
        if value is None:
            raise KeyError(key)
        return json.loads(value.decode('utf-8'))

    def __setitem__(self, key, value):
        self._redis.hset(self._key, key, json.dumps(value))

    def __delitem__(self, key):
        success = self._redis.hdel(self._key, key)
        if not bool(success):
            raise KeyError(key)

    def __iter__(self):
        return super().__iter__(self._redis.hscan)

    def __len__(self):
        return self._redis.hlen(self._key)
