# --------------------------------------------------------------------------- #
#   base.py                                                                   #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#                                                                             #
#   Licensed under the Apache License, Version 2.0 (the "License");           #
#   you may not use this file except in compliance with the License.          #
#   You may obtain a copy of the License at:                                  #
#       http://www.apache.org/licenses/LICENSE-2.0                            #
#                                                                             #
#   Unless required by applicable law or agreed to in writing, software       #
#   distributed under the License is distributed on an "AS IS" BASIS,         #
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  #
#   See the License for the specific language governing permissions and       #
#   limitations under the License.                                            #
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
from types import TracebackType
from typing import Any
from typing import AnyStr
from typing import ClassVar
from typing import ContextManager
from typing import FrozenSet
from typing import Generator
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union
from typing import cast
from typing import overload

from redis import Redis
from redis import RedisError
from redis.client import Pipeline
from typing_extensions import Final
from typing_extensions import Literal

from . import monkey
from .annotations import JSONTypes
from .annotations import RedisValues
from .exceptions import QuorumIsImpossible
from .exceptions import RandomKeyError


_default_url: Final[str] = os.environ.get('REDIS_URL', 'redis://localhost:6379/')
_default_redis: Final[Redis] = Redis.from_url(_default_url, socket_timeout=1)
_logger: Final[logging.Logger] = logging.getLogger('pottery')


def random_key(*,
               redis: Redis,
               prefix: str = 'pottery:',
               length: int = 16,
               num_tries: int = 3,
               ) -> str:
    'Find/return a random key that does not exist in the Redis instance.'
    if not isinstance(num_tries, int):
        raise TypeError('num_tries must be an int >= 0')
    if num_tries < 0:
        raise ValueError('num_tries must be an int >= 0')
    if num_tries == 0:
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
            num_tries=num_tries-1,
        )
    return key


class _Common:
    'Mixin class that implements self.redis and self.key properties.'

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
            _logger.warning(
                "Deleted tmp <%s key='%s'> (instance is about to be destroyed)",
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
        _logger.warning(
            "Self-assigning tmp key <%s key='%s'>",
            self.__class__.__name__,
            key,
        )
        return key

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __random_key = _random_key


class _Encodable:
    'Mixin class that implements JSON encoding and decoding.'

    @staticmethod
    def _encode(value: JSONTypes) -> str:
        encoded = json.dumps(value, sort_keys=True)
        return encoded

    @staticmethod
    def _decode(value: AnyStr) -> JSONTypes:
        try:
            string = cast(bytes, value).decode('utf-8')
        except AttributeError:
            string = cast(str, value)
        decoded: JSONTypes = json.loads(string)
        return decoded


class _Comparable(metaclass=abc.ABCMeta):
    'Mixin class that implements equality testing for Redis-backed collections.'

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
            return True
        if (
            isinstance(other, _Comparable)
            and self.redis.connection_pool == other.redis.connection_pool  # NoQA: W503
            and self.key == other.key  # NoQA: W503
        ):
            return True
        equals = super().__eq__(other)
        if equals is NotImplemented:
            equals = False
        return equals


class _Clearable(metaclass=abc.ABCMeta):
    'Mixin class that implements clearing (emptying) a Redis-backed collection.'

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
    def __watch_keys(self,
                     *keys: str,
                     ) -> Generator[Pipeline, None, None]:
        with self.redis.pipeline() as pipeline:
            pipeline.watch(*keys)
            try:
                yield pipeline
            except Exception as error:
                _logger.warning(
                    'Caught %s; aborting pipeline of %d commands',
                    error.__class__.__name__,
                    len(pipeline),
                )
                raise
            else:
                _logger.info(
                    'Running EXEC on pipeline of %d commands',
                    len(pipeline),
                )
                pipeline.execute()

    def __context_managers(self,
                           *others: Any,
                           ) -> Generator[ContextManager[Pipeline], None, None]:
        redises = collections.defaultdict(list)
        for container in itertools.chain((self,), others):
            if isinstance(container, _Pipelined):
                connection_kwargs = frozenset(
                    container.redis.connection_pool.connection_kwargs.items(),
                )
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
    'Base class for Redis-backed collections.'


class Iterable_(metaclass=abc.ABCMeta):
    'Mixin class that implements iterating over a Redis-backed collection.'

    @staticmethod
    @abc.abstractmethod
    def _decode(value: bytes) -> JSONTypes:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def key(self) -> str:
        'Redis key.'
        raise NotImplementedError

    @abc.abstractmethod
    def _scan(self,
              *,
              cursor: int = 0,
              ) -> Tuple[int, Union[List[bytes], Mapping[bytes, bytes]]]:
        raise NotImplementedError

    def __iter__(self) -> Generator[JSONTypes, None, None]:
        'Iterate over the items in a Redis-backed container.  O(n)'
        cursor = 0
        while True:
            cursor, iterable = self._scan(cursor=cursor)
            yield from (self._decode(value) for value in iterable)
            if cursor == 0:
                break


class Primitive(metaclass=abc.ABCMeta):
    'Base class for Redis-backed distributed primitives.'

    __slots__ = ('_key', 'masters', 'raise_on_redis_errors')

    _DEFAULT_MASTERS: ClassVar[FrozenSet[Redis]] = frozenset({_default_redis})

    def __init__(self,
                 *,
                 key: str,
                 masters: Iterable[Redis] = frozenset(),
                 raise_on_redis_errors: bool = False,
                 ) -> None:
        self.key = key
        self.masters = frozenset(masters) or self._DEFAULT_MASTERS
        self.raise_on_redis_errors = raise_on_redis_errors

    @property
    @abc.abstractmethod
    def KEY_PREFIX(self) -> str:
        'Redis key prefix/namespace.'

    @property
    def key(self) -> str:
        return self._key

    @key.setter
    def key(self, value: str) -> None:
        self._key = f'{self.KEY_PREFIX}:{value}'

    def _check_enough_masters_up(self,
                                 raise_on_redis_errors: Optional[bool],
                                 redis_errors: List[RedisError],
                                 ) -> None:
        if raise_on_redis_errors is None:
            raise_on_redis_errors = self.raise_on_redis_errors
        if raise_on_redis_errors and len(redis_errors) > len(self.masters) // 2:
            raise QuorumIsImpossible(
                self.key,
                self.masters,
                redis_errors=redis_errors,
            )
