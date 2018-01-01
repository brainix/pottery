#-----------------------------------------------------------------------------#
#   base.py                                                                   #
#                                                                             #
#   Copyright © 2015-2018, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import abc
import collections
import contextlib
import functools
import itertools
import json
import os
import random
import string

from redis import Redis
from redis import RedisError

from . import monkey
from .exceptions import RandomKeyError



monkey  # Workaround for Pyflakes.  :-(
_default_url = os.environ.get('REDIS_URL', 'http://localhost:6379/')
_default_redis = Redis.from_url(_default_url, socket_timeout=1)



class _Common:
    _NUM_TRIES = 3
    _RANDOM_KEY_PREFIX = 'pottery:'
    _RANDOM_KEY_LENGTH = 16

    def __init__(self, *args, redis=None, key=None, **kwargs):
        self.redis = redis
        self.key = key

    def __del__(self):
        if self.key.startswith(self._RANDOM_KEY_PREFIX):
            self.redis.delete(self.key)

    @property
    def redis(self):
        return self._redis

    @redis.setter
    def redis(self, value):
        self._redis = _default_redis if value is None else value

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = value or self._random_key()

    def _random_key(self, *, tries=_NUM_TRIES):
        if tries <= 0:
            raise RandomKeyError(self.redis, self.key)
        all_chars = string.digits + string.ascii_letters
        random_char = functools.partial(random.choice, all_chars)
        suffix = ''.join(random_char() for n in range(self._RANDOM_KEY_LENGTH))
        random_key = self._RANDOM_KEY_PREFIX + suffix
        if self.redis.exists(random_key):
            random_key = self._random_key(tries=tries-1)
        return random_key



class _Encodable:
    @staticmethod
    def _encode(value):
        encoded = json.dumps(value, sort_keys=True)
        return encoded

    @staticmethod
    def _decode(value):
        decoded = json.loads(value.decode('utf-8'))
        return decoded



class _Comparable(metaclass=abc.ABCMeta):
    @abc.abstractproperty
    def redis(self):
        'Redis client.'

    @abc.abstractproperty
    def key(self):
        'Redis key.'

    def __eq__(self, other):
        if self is other:
            equals = True
        elif isinstance(other, _Comparable) and \
           self.redis == other.redis and \
           self.key == other.key:
            equals = True
        else:
            equals = super().__eq__(other)
            if equals is NotImplemented:
                equals = False
        return equals



class _Clearable(metaclass=abc.ABCMeta):
    @abc.abstractproperty
    def redis(self):
        'Redis client.'

    @abc.abstractproperty
    def key(self):
        'Redis key.'

    def clear(self):
        'Remove the elements in a Redis-backed container.  O(n)'
        self.redis.delete(self.key)



class Pipelined(metaclass=abc.ABCMeta):
    @abc.abstractproperty
    def _NUM_TRIES(self):
        'The number of times to try generating a random key before giving up.'

    @abc.abstractproperty
    def redis(self):
        'Redis client.'

    @abc.abstractproperty
    def key(self):
        'Redis key.'

    @contextlib.contextmanager
    def _pipeline(self):
        pipeline = self.redis.pipeline()
        yield pipeline
        with contextlib.suppress(RedisError):
            pipeline.multi()
        pipeline.ping()
        pipeline.execute()

    @contextlib.contextmanager
    def _watch_keys(self, *keys):
        original_redis = self.redis
        keys = keys or (self.key,)
        try:
            with self._pipeline() as pipeline:
                self.redis = pipeline
                pipeline.watch(*keys)
                yield pipeline
        finally:
            self.redis = original_redis

    def _context_managers(self, *others):
        redises = collections.defaultdict(list)
        for container in itertools.chain((self,), others):
            if isinstance(container, Base):
                redises[container.redis].append(container)
        for containers in redises.values():
            keys = (container.key for container in containers)
            yield containers[0]._watch_keys(*keys)

    @contextlib.contextmanager
    def _watch(self, *others):
        with contextlib.ExitStack() as stack:
            for context_manager in self._context_managers(*others):
                stack.enter_context(context_manager)
            yield



class Base(_Common, _Encodable, _Comparable, _Clearable, Pipelined):
    ...



class Iterable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def _decode(value):     # pragma: no cover
        ...

    @abc.abstractproperty   # pragma: no cover
    def key(self):
        'Redis key.'

    @abc.abstractmethod     # pragma: no cover
    def _scan(self, key, *, cursor=0):
        ...

    def __iter__(self):
        'Iterate over the items in a Redis-backed container.  O(n)'
        cursor = 0
        while True:
            cursor, iterable = self._scan(self.key, cursor=cursor)
            yield from (self._decode(value) for value in iterable)
            if cursor == 0:
                break



class Primitive(metaclass=abc.ABCMeta):
    _default_masters = frozenset({_default_redis})

    def __init__(self, *, key, masters=_default_masters):
        self.key = key
        self.masters = masters or self._default_masters

    @abc.abstractproperty
    def KEY_PREFIX(self):
        'Redis key prefix/namespace.'

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = '{}:{}'.format(self.KEY_PREFIX, value)
