#-----------------------------------------------------------------------------#
#   base.py                                                                   #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import abc
import contextlib
import functools
import json
import os
import random
import string

from redis import Redis
from redis import WatchError

from .exceptions import RandomKeyError
from .exceptions import TooManyTriesError



class Base:
    _DEFAULT_REDIS_URL = 'http://localhost:6379/'
    _NUM_TRIES = 3
    _RANDOM_KEY_PREFIX = 'pottery-'
    _RANDOM_KEY_LENGTH = 16

    @staticmethod
    def _encode(value):
        return json.dumps(value)

    @staticmethod
    def _decode(value):
        return json.loads(value.decode('utf-8'))

    @classmethod
    def _watch(cls):
        def wrap1(func):
            @functools.wraps(func)
            def wrap2(self, *args, **kwargs):
                for _ in range(cls._NUM_TRIES):
                    try:
                        original_redis = self.redis
                        self.redis = self.redis.pipeline()
                        self.redis.watch(self.key)
                        value = func(self, *args, **kwargs)
                        self.redis.execute()
                        return value
                    except WatchError:
                        pass
                    finally:
                        self.redis = original_redis
                else:
                    raise TooManyTriesError(self.redis, self.key)
            return wrap2
        return wrap1

    @classmethod
    def _default_redis(cls):
        url = os.environ.get('REDISCLOUD_URL', cls._DEFAULT_REDIS_URL)
        return Redis.from_url(url)

    def __init__(self, *args, redis=None, key=None, **kwargs):
        self.redis = redis
        self.key = key

    @property
    def redis(self):
        return self._redis

    @redis.setter
    def redis(self, value):
        self._redis = self._default_redis() if value is None else value

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = self._random_key() if value is None else value

    def _random_key(self, tries=_NUM_TRIES):
        if tries <= 0:
            raise RandomKeyError(self.redis, self.key)
        chars = string.digits + string.ascii_letters
        random_char = functools.partial(random.choice, chars)
        suffix = ''.join(random_char() for n in range(self._RANDOM_KEY_LENGTH))
        value = self._RANDOM_KEY_PREFIX + suffix
        return _random_key(tries - 1) if self.redis.exists(value) else value

    @contextlib.contextmanager
    def _pipeline(self):
        pipeline = self.redis.pipeline()
        try:
            yield pipeline
        finally:
            pipeline.execute()



class Iterable(metaclass=abc.ABCMeta):
    def __iter__(self):
        """Iterate over the items in a Redis-backed container.  O(n)"""
        cursor = 0
        while True:
            cursor, iterable = self._scan(self.key, cursor=cursor)
            for value in iterable:
                yield self._decode(value)
            if cursor == 0:
                break

    @abc.abstractmethod
    def _scan(self, key, *, cursor=0):
        ...
