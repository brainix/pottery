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
from typing import Any
from typing import ClassVar
from typing import ContextManager
from typing import Dict
from typing import FrozenSet
from typing import Generator
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Union
from typing import cast

from redis import Redis
from redis import RedisError
from redis.client import Pipeline
from typing_extensions import Final

from . import monkey
from .exceptions import RandomKeyError


_default_url: Final[str] = os.environ.get('REDIS_URL', 'redis://localhost:6379/')
_default_redis: Final[Redis] = Redis.from_url(_default_url, socket_timeout=1)
_logger: Final[logging.Logger] = logging.getLogger('pottery')


def random_key(*,
               redis: Redis,
               prefix: str = 'pottery:',
               length: int = 16,
               tries: int = 3,
               ) -> str:
    if tries <= 0:
        raise RandomKeyError(redis)
    all_chars = string.digits + string.ascii_letters
    random_char = functools.partial(random.choice, all_chars)
    suffix = ''.join(cast(str, random_char()) for n in range(length))
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
    _RANDOM_KEY_PREFIX: ClassVar[str] = 'pottery:'

    def __init__(self,
                 *,
                 redis: Optional[Redis] = None,
                 key: Optional[str] = None,
                 ) -> None:
        self.redis = cast(Redis, redis)
        self.key = cast(str, key)

    def __del__(self) -> None:
        if self.key.startswith(self._RANDOM_KEY_PREFIX):
            self.redis.delete(self.key)
            _logger.info(
                "Deleted tmp <%s key='%s'> (went out of scope)",
                self.__class__.__name__,
                self.key,
            )

    @property
    def redis(self) -> Redis:
        return self._redis

    @redis.setter
    def redis(self, value: Optional[Redis]) -> None:
        self._redis = _default_redis if value is None else value

    @property
    def key(self) -> str:
        return self._key  # type: ignore

    @key.setter
    def key(self, value: str) -> None:
        self._key = value or self.__random_key()

    def _random_key(self) -> str:
        key = random_key(redis=self.redis, prefix=self._RANDOM_KEY_PREFIX)
        _logger.info(
            "Self-assigning tmp key <%s key='%s'>",
            self.__class__.__name__,
            key,
        )
        return key

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __random_key = _random_key


JSONTypes = Union[None, bool, int, float, str, List[Any], Dict[str, Any]]
RedisValues = Union[bytes, str, float, int]


class _Encodable:
    @staticmethod
    def _encode(value: JSONTypes) -> str:
        encoded = json.dumps(value, sort_keys=True)
        return encoded

    @staticmethod
    def _decode(value: bytes) -> JSONTypes:
        decoded: JSONTypes = json.loads(value.decode('utf-8'))
        return decoded


class _Comparable(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def redis(self) -> Redis:
        'Redis client.'

    @property
    @abc.abstractmethod
    def key(self) -> str:
        'Redis key.'

    def __eq__(self, other: Any) -> bool:
        if self is other:
            equals = True
        elif (
            isinstance(other, _Comparable)
            and self.redis.connection_pool == other.redis.connection_pool  # NoQA: W503
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
    def redis(self) -> Redis:
        'Redis client.'

    @property
    @abc.abstractmethod
    def key(self) -> str:
        'Redis key.'

    def clear(self) -> None:
        'Remove the elements in a Redis-backed container.  O(n)'
        self.redis.delete(self.key)


class _Pipelined(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def redis(self) -> Redis:
        'Redis client.'

    @property
    @abc.abstractmethod
    def key(self) -> str:
        'Redis key.'

    @contextlib.contextmanager
    def __pipeline(self) -> Generator[Pipeline, None, None]:
        pipeline = self.redis.pipeline()
        yield pipeline
        with contextlib.suppress(RedisError):
            pipeline.multi()
        pipeline.ping()
        pipeline.execute()

    @contextlib.contextmanager
    def __watch_keys(self,
                     *keys: Iterable[str],
                     ) -> Generator[Pipeline, None, None]:
        with self.__pipeline() as pipeline:
            pipeline.watch(*keys)
            yield pipeline

    def __context_managers(self,
                           *others: Any,
                           ) -> Generator[ContextManager[Pipeline], None, None]:
        redises = collections.defaultdict(list)
        for container in itertools.chain((self,), others):
            if isinstance(container, _Pipelined):
                connection_kwargs = frozenset(container.redis.connection_pool.connection_kwargs.items())
                redises[connection_kwargs].append(container)
        for containers in redises.values():
            keys = (container.key for container in containers)
            pipeline = containers[0].__watch_keys(*keys)
            yield pipeline

    @contextlib.contextmanager
    def _watch(self,
               *others: Any,
               ) -> Generator[Pipeline, None, None]:
        pipelines = []
        with contextlib.ExitStack() as stack:
            for context_manager in self.__context_managers(*others):
                pipeline = stack.enter_context(context_manager)
                pipelines.append(pipeline)
            yield pipelines[0]


class Base(_Common, _Encodable, _Comparable, _Clearable, _Pipelined):
    ...


class Iterable_(metaclass=abc.ABCMeta):
    @staticmethod  # pragma: no cover
    @abc.abstractmethod
    def _decode(value: bytes) -> JSONTypes:
        ...

    @property  # pragma: no cover
    @abc.abstractmethod
    def key(self) -> str:
        'Redis key.'

    @abc.abstractmethod  # pragma: no cover
    def _scan(self,
              *, cursor: int = 0,
              ) -> Tuple[int, Union[List[bytes], Mapping[bytes, bytes]]]:
        ...

    def __iter__(self) -> Generator[JSONTypes, None, None]:
        'Iterate over the items in a Redis-backed container.  O(n)'
        cursor = 0
        while True:
            cursor, iterable = self._scan(cursor=cursor)
            yield from (self._decode(value) for value in iterable)
            if cursor == 0:
                break


class Primitive(metaclass=abc.ABCMeta):
    _DEFAULT_MASTERS: ClassVar[FrozenSet[Redis]] = frozenset({_default_redis})

    def __init__(self,
                 *,
                 key: str,
                 masters: Iterable[Redis] = frozenset(),
                 ) -> None:
        self.key = key
        self.masters = frozenset(masters) or self._DEFAULT_MASTERS

    @property
    @abc.abstractmethod
    def KEY_PREFIX(self) -> str:
        'Redis key prefix/namespace.'

    @property
    def key(self) -> str:
        return self._key

    @key.setter
    def key(self, value: str) -> None:
        self._key = '{}:{}'.format(self.KEY_PREFIX, value)
