# --------------------------------------------------------------------------- #
#   base.py                                                                   #
#                                                                             #
#   Copyright © 2015-2022, Rajiv Bakulesh Shah, original author.              #
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


# TODO: When we drop support for Python 3.9, remove the following import.  We
# only need it for X | Y union type annotations as of 2022-01-29.
from __future__ import annotations

import abc
import collections
import contextlib
import json
import os
import uuid
import warnings
from typing import Any
from typing import AnyStr
from typing import ClassVar
from typing import ContextManager
from typing import FrozenSet
from typing import Generator
from typing import Iterable
from typing import List
from typing import Tuple
from typing import cast

from redis import Redis
from redis import RedisError
from redis.client import Pipeline
# TODO: When we drop support for Python 3.7, change the following imports to:
#   from typing import Final
#   from typing import Protocol
#   from typing import final
from typing_extensions import Final
from typing_extensions import Protocol
from typing_extensions import final

from .annotations import JSONTypes
from .exceptions import InefficientAccessWarning
from .exceptions import QuorumIsImpossible
from .exceptions import RandomKeyError
from .monkey import logger


_default_url: Final[str] = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
_default_redis: Final[Redis] = Redis.from_url(_default_url, socket_timeout=1)


def random_key(*,
               redis: Redis,
               prefix: str = 'pottery:',
               num_tries: int = 3,
               ) -> str:
    'Find/return a random key that does not exist in the Redis instance.'
    if not isinstance(num_tries, int):
        raise TypeError('num_tries must be an int >= 0')
    if num_tries < 0:
        raise ValueError('num_tries must be an int >= 0')
    if num_tries == 0:
        raise RandomKeyError(redis)

    uuid4 = str(uuid.uuid4())
    key = prefix + uuid4
    if redis.exists(key):  # Available since Redis 1.0.0
        key = random_key(
            redis=redis,
            prefix=prefix,
            num_tries=num_tries-1,
        )
    return key


def connection_args(redis: Redis) -> Tuple[str, int, int]:
    return (
        redis.connection_pool.connection_kwargs['host'],
        redis.connection_pool.connection_kwargs.get('port', 6379),
        redis.connection_pool.connection_kwargs.get('db', 0),
    )


class _Common:
    'Mixin class that implements self.redis and self.key properties.'

    _RANDOM_KEY_PREFIX: ClassVar[str] = 'pottery:'

    def __init__(self, *, redis: Redis | None = None, key: str = '') -> None:
        self.redis = redis or _default_redis
        self.key = key or self.__random_key()

    def __del__(self) -> None:
        if self.key.startswith(self._RANDOM_KEY_PREFIX):
            self.redis.unlink(self.key)  # Available since Redis 4.0.0
            logger.warning(
                "Deleted tmp <%s key='%s'> (instance is about to be destroyed)",
                self.__class__.__name__,
                self.key,
            )

    @property
    def redis(self) -> Redis:
        # Preserve the Open-Closed Principle with name mangling.
        #   https://youtu.be/miGolgp9xq8?t=2086
        #   https://stackoverflow.com/a/38534939
        return self.__redis

    @redis.setter
    def redis(self, value: Redis | None) -> None:
        self.__redis = value or _default_redis

    @property
    def key(self) -> str:
        return self.__key  # type: ignore

    @key.setter
    def key(self, value: str) -> None:
        self.__key = value or self.__random_key()

    def _random_key(self) -> str:
        key = random_key(redis=self.redis, prefix=self._RANDOM_KEY_PREFIX)
        logger.warning(
            "Self-assigning tmp key <%s key='%s'>",
            self.__class__.__name__,
            key,
        )
        return key

    __random_key = _random_key


class _Encodable:
    'Mixin class that implements JSON encoding and decoding.'

    @final
    @staticmethod
    def _encode(decoded_value: JSONTypes) -> str:
        encoded_value = json.dumps(decoded_value, sort_keys=True)
        return encoded_value

    @final
    @staticmethod
    def _decode(encoded_value: AnyStr) -> JSONTypes:
        if isinstance(encoded_value, bytes):
            string = encoded_value.decode()
        else:
            string = encoded_value
        decoded_value: JSONTypes = json.loads(string)
        return decoded_value


class _HasRedisClientAndKey(Protocol):
    @property
    def redis(self) -> Redis:  # pragma: no cover
        ...

    @property
    def key(self) -> str:  # pragma: no cover
        ...


class _Clearable:
    'Mixin class that implements clearing (emptying) a Redis-backed collection.'
    def clear(self: _HasRedisClientAndKey) -> None:
        'Remove the elements in a Redis-backed container.  O(n)'
        self.redis.unlink(self.key)  # Available since Redis 4.0.0


class _Pipelined(abc.ABC):
    @property
    @abc.abstractmethod
    def redis(self) -> Redis:
        'Redis client.'

    @property
    @abc.abstractmethod
    def key(self) -> str:
        'Redis key.'

    @final
    @contextlib.contextmanager
    def __watch_keys(self, *keys: str) -> Generator[Pipeline, None, None]:
        with self.redis.pipeline() as pipeline:
            pipeline.watch(*keys)  # Available since Redis 2.2.0
            try:
                yield pipeline
            except Exception as error:
                logger.warning(
                    'Caught %s; aborting pipeline of %d commands',
                    error.__class__.__name__,
                    len(pipeline),
                )
                raise
            else:
                logger.info(
                    'Running EXEC on pipeline of %d commands',
                    len(pipeline),
                )
                pipeline.execute()  # Available since Redis 1.2.0

    @final
    def __context_managers(self,
                           *others: Any,
                           ) -> Generator[ContextManager[Pipeline], None, None]:
        redises = collections.defaultdict(list)
        for container in (self, *others):
            if isinstance(container, _Pipelined):
                redises[connection_args(container.redis)].append(container)
        for containers in redises.values():
            keys = (container.key for container in containers)
            pipeline = containers[0].__watch_keys(*keys)
            yield pipeline

    @final
    @contextlib.contextmanager
    def _watch(self, *others: Any) -> Generator[Pipeline, None, None]:
        'Watch self and others, and yield a Redis pipeline.'
        pipelines = []
        with contextlib.ExitStack() as stack:
            for context_manager in self.__context_managers(*others):
                pipeline = stack.enter_context(context_manager)
                pipelines.append(pipeline)
            yield pipelines[0]


class _Comparable(abc.ABC):
    'Mixin class that implements equality testing for Redis-backed collections.'

    @property
    @abc.abstractmethod
    def redis(self) -> Redis:
        'Redis client.'

    @property
    @abc.abstractmethod
    def key(self) -> str:
        'Redis key.'

    @abc.abstractmethod
    @contextlib.contextmanager
    def _watch(self, *others: Any) -> Generator[Pipeline, None, None]:
        'Watch self and others, and yield a Redis pipeline.'

    @final
    def _same_redis(self, *others: Any) -> bool:
        for other in others:
            if not isinstance(other, _Comparable):
                return False
            if connection_args(self.redis) != connection_args(other.redis):
                return False
        return True

    def __eq__(self, other: Any) -> bool:
        if self is other:
            return True
        if self._same_redis(other) and self.key == other.key:
            return True

        warnings.warn(
            cast(str, InefficientAccessWarning.__doc__),
            InefficientAccessWarning,
        )
        with self._watch(other):
            equals = super().__eq__(other)
        if equals is NotImplemented:
            equals = False
        return equals


class Container(_Common, _Encodable, _Clearable, _Pipelined, _Comparable):
    'Base class for Redis-backed collections.'


class Iterable_(abc.ABC):
    'Mixin class that implements iterating over a Redis-backed collection.'

    @abc.abstractmethod
    def __iter__(self) -> Generator[JSONTypes, None, None]:
        raise NotImplementedError


class Primitive(abc.ABC):
    'Base class for Redis-backed distributed primitives.'

    @property
    @abc.abstractmethod
    def _KEY_PREFIX(self) -> str:
        'Redis key prefix/namespace.'

    __slots__ = ('__key', 'masters', 'raise_on_redis_errors')

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
    def key(self) -> str:
        return self.__key

    @key.setter
    def key(self, value: str) -> None:
        self.__key = f'{self._KEY_PREFIX}:{value}'

    def _check_enough_masters_up(self,
                                 raise_on_redis_errors: bool | None,
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
