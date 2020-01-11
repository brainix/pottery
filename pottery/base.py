# --------------------------------------------------------------------------- #
#   base.py                                                                   #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import abc
import collections
import contextlib
import functools
import itertools
import json
import logging
import os
import random
import string

from redis import Redis
from redis import RedisError

from . import monkey
from .exceptions import RandomKeyError


_default_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/')
_default_redis = Redis.from_url(_default_url, socket_timeout=1)
_logger = logging.getLogger('pottery')


def random_key(*, redis, prefix='pottery:', length=16, tries=3):
    if tries <= 0:
        raise RandomKeyError(redis)
    all_chars = string.digits + string.ascii_letters
    random_char = functools.partial(random.choice, all_chars)
    suffix = ''.join(random_char() for n in range(length))
    key = prefix + suffix
    if redis.exists(key):
        key = random_key(
            redis=redis,
            prefix=prefix,
            length=length,
            tries=tries-1,
        )
    return key


class _Common:
    _RANDOM_KEY_PREFIX = 'pottery:'

    def __init__(self, *args, redis=None, key=None, **kwargs):
        self.redis = redis
        self.key = key

    def __del__(self):
        if self.key.startswith(self._RANDOM_KEY_PREFIX):
            self.redis.delete(self.key)
            _logger.info(
                "Deleted tmp <%s key='%s'> (went out of scope)",
                self.__class__.__name__,
                self.key,
            )

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

    def _random_key(self):
        key = random_key(redis=self.redis, prefix=self._RANDOM_KEY_PREFIX)
        _logger.info(
            "Self-assigning tmp key <%s key='%s'>",
            self.__class__.__name__,
            key,
        )
        return key


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
    @property
    @abc.abstractmethod
    def redis(self):
        'Redis client.'

    @property
    @abc.abstractmethod
    def key(self):
        'Redis key.'

    def __eq__(self, other):
        if self is other:
            equals = True
        elif (
            isinstance(other, _Comparable)
            and self.redis == other.redis  # NoQA: W503
            and self.key == other.key  # NoQA: W503
        ):
            equals = True
        else:
            equals = super().__eq__(other)
            if equals is NotImplemented:
                equals = False
        return equals


class _Clearable(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def redis(self):
        'Redis client.'

    @property
    @abc.abstractmethod
    def key(self):
        'Redis key.'

    def clear(self):
        'Remove the elements in a Redis-backed container.  O(n)'
        self.redis.delete(self.key)


class Pipelined(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def redis(self):
        'Redis client.'

    @property
    @abc.abstractmethod
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
    def _decode(value):  # pragma: no cover
        ...

    @property  # pragma: no cover
    @abc.abstractmethod
    def key(self):
        'Redis key.'

    @abc.abstractmethod  # pragma: no cover
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
    _DEFAULT_MASTERS = frozenset({_default_redis})

    def __init__(self, *, key, masters=frozenset()):
        self.key = key
        self.masters = masters or self._DEFAULT_MASTERS

    @property
    @abc.abstractmethod
    def KEY_PREFIX(self):
        'Redis key prefix/namespace.'

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = '{}:{}'.format(self.KEY_PREFIX, value)
