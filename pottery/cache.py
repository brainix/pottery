# --------------------------------------------------------------------------- #
#   cache.py                                                                  #
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

import collections
import functools
import itertools
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Collection
from typing import Hashable
from typing import Iterable
from typing import Mapping
from typing import NamedTuple
from typing import Tuple
from typing import TypeVar
from typing import Union
from typing import cast

from redis import Redis
from redis.exceptions import WatchError
# TODO: When we drop support for Python 3.7, change the following import to:
#   from typing import Final
from typing_extensions import Final

from .annotations import JSONTypes
from .base import _default_redis
from .base import logger
from .base import random_key
from .dict import RedisDict


F = TypeVar('F', bound=Callable[..., JSONTypes])

UpdateMap = Mapping[JSONTypes, Union[JSONTypes, object]]
UpdateItem = Tuple[JSONTypes, Union[JSONTypes, object]]
UpdateIter = Iterable[UpdateItem]
UpdateArg = Union[UpdateMap, UpdateIter]

_DEFAULT_TIMEOUT: Final[int] = 60   # seconds


class CacheInfo(NamedTuple):
    '''Caching decorator information.

    This CacheInfo named tuple is compatible with the one in functools:
        https://github.com/python/cpython/blob/7a34380ad788886f5ad50d4175ceb2d5715b8cff/Lib/functools.py#L430
    '''
    hits: int = 0
    misses: int = 0
    maxsize: int | None = None
    currsize: int = 0


def _arg_hash(*args: Hashable, **kwargs: Hashable) -> int:
    kwargs_items = frozenset(kwargs.items())
    return hash((args, kwargs_items))


def redis_cache(*,  # NoQA: C901
                redis: Redis | None = None,
                key: str | None = None,
                timeout: int | None = _DEFAULT_TIMEOUT,
                ) -> Callable[[F], F]:
    '''Redis-backed caching decorator with an API like functools.lru_cache().

    Arguments to the original underlying function must be hashable, and return
    values from the function must be JSON serializable.

    Additionally, this decorator provides the following functions:

    f.__wrapped__(*args, **kwargs)
        Access the original underlying function.  This is useful for
        introspection, for bypassing the cache, or for rewrapping the function
        with a different cache.

    f.__bypass__(*args, **kwargs)
        Force a cache reset for your args/kwargs.  Bypass the cache lookup,
        call the original underlying function, then cache the results for
        future calls to f(*args, **kwargs).

    f.cache_info()
        Return a NamedTuple showing hits, misses, maxsize, and currsize.  This
        information is helpful for measuring the effectiveness of the cache.

        Note that maxsize is always None, meaning that this cache is always
        unbounded.  maxsize is only included for compatibility with
        functools.lru_cache().

        While redis_cache() is thread-safe, also note that hits/misses only
        instrument your local process - not other processes, even if connected
        to the same Redis-backed redis_cache() key.  And in some cases,
        hits/misses may be incorrect in multiprocess/distributed applications.

        That said, currsize is always correct, even if other remote processes
        modify the same Redis-backed redis_cache() key.

    f.cache_clear()
        Clear/invalidate the entire cache (for all args/kwargs previously
        cached) for your function.

    In general, you should only use redis_cache() when you want to reuse
    previously computed values.  Accordingly, it doesn't make sense to cache
    functions with side-effects or impure functions such as time() or random().

    However, unlike functools.lru_cache(), redis_cache() reconstructs
    previously cached objects on each cache hit.  Therefore, you can use
    redis_cache() for a function that needs to create a distinct mutable object
    on each call.
    '''

    if redis is None:
        redis = _default_redis

    def decorator(func: F) -> F:
        nonlocal redis, key
        if key is None:
            key = random_key(redis=cast(Redis, redis))
            logger.warning(
                "Self-assigning key redis_cache(key='%s') for function %s",
                key,
                func.__qualname__,
            )
        cache = RedisDict(redis=redis, key=key)
        hits, misses = 0, 0

        @functools.wraps(func)
        def wrapper(*args: Hashable, **kwargs: Hashable) -> JSONTypes:
            nonlocal hits, misses
            hash_ = _arg_hash(*args, **kwargs)
            try:
                return_value = cache[hash_]
                hits += 1
            except KeyError:
                return_value = func(*args, **kwargs)
                cache[hash_] = return_value
                misses += 1
            if timeout:
                cast(Redis, redis).expire(cast(str, key), timeout)
            return return_value

        @functools.wraps(func)
        def bypass(*args: Hashable, **kwargs: Hashable) -> JSONTypes:
            hash_ = _arg_hash(*args, **kwargs)
            return_value = func(*args, **kwargs)
            cache[hash_] = return_value
            if timeout:
                cast(Redis, redis).expire(cast(str, key), timeout)
            return return_value

        def cache_info() -> CacheInfo:
            return CacheInfo(
                hits=hits,
                misses=misses,
                maxsize=None,
                currsize=len(cache),
            )

        def cache_clear() -> None:
            nonlocal hits, misses
            cast(Redis, redis).unlink(cast(str, key))
            hits, misses = 0, 0

        wrapper.__wrapped__ = func  # type: ignore
        wrapper.__bypass__ = bypass  # type: ignore
        wrapper.cache_info = cache_info  # type: ignore
        wrapper.cache_clear = cache_clear  # type: ignore
        return cast(F, wrapper)
    return decorator


def _set_expiration(func: F) -> F:
    @functools.wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        value = func(self, *args, **kwargs)
        if self._timeout:
            self._cache.redis.expire(self._cache.key, self._timeout)  # Available since Redis 1.0.0
        return value
    return cast(F, wrapper)


class CachedOrderedDict(collections.OrderedDict):
    '''Redis-backed container that extends Python's OrderedDicts.

    The best way that I can explain CachedOrderedDict is through an example
    use-case.  Imagine that your search engine returns document IDs, which then
    you have to hydrate into full documents via the database to return to the
    client.  The data structure used to represent such search results must have
    the following properties:

        1. It must preserve the order of the document IDs returned by the
           search engine
        2. It must map document IDs to hydrated documents
        3. It must cache previously hydrated documents

    Properties 1 and 2 are satisfied by Python's collections.OrderedDict.
    However, CachedOrderedDict extends Python's OrderedDict to also satisfy
    property 3.
    '''

    _SENTINEL: ClassVar[object] = object()
    _NUM_TRIES: ClassVar[int] = 3

    @_set_expiration
    def __init__(self,
                 *,
                 redis_client: Redis | None = None,
                 redis_key: str | None = None,
                 dict_keys: Iterable[JSONTypes] = tuple(),
                 num_tries: int = _NUM_TRIES,
                 timeout: int | None = _DEFAULT_TIMEOUT,
                 ) -> None:
        self._num_tries = num_tries
        self._timeout = timeout
        init_cache = functools.partial(
            RedisDict,
            redis=redis_client,
            key=redis_key,
        )
        self._cache = self.__retry(init_cache)
        self._misses = set()

        # We have to iterate over dict_keys multiple times, so cast it to a
        # tuple.  This allows the caller to pass in a generator for dict_keys,
        # and we can still iterate over it multiple times.
        items, dict_keys = [], tuple(dict_keys)
        if dict_keys:
            encoded_keys = (
                self._cache._encode(dict_key) for dict_key in dict_keys
            )
            encoded_values = self._cache.redis.hmget(  # Available since Redis 2.0.0
                self._cache.key,
                *encoded_keys,
            )
            for dict_key, encoded_value in zip(dict_keys, encoded_values):
                if encoded_value is None:
                    self._misses.add(dict_key)
                    value = self._SENTINEL
                else:
                    value = self._cache._decode(encoded_value)
                item = (dict_key, value)
                items.append(item)
        super().__init__()
        self.__update(items)

    def misses(self) -> Collection[JSONTypes]:
        return frozenset(self._misses)

    @_set_expiration
    def __setitem__(self,
                    dict_key: JSONTypes,
                    value: JSONTypes | object,
                    ) -> None:
        'Set self[dict_key] to value.'
        if value is not self._SENTINEL:  # pragma: no cover
            self._cache[dict_key] = value
            self._misses.discard(dict_key)
        super().__setitem__(dict_key, value)

    @_set_expiration
    def setdefault(self,
                   dict_key: JSONTypes,
                   default: JSONTypes = None,
                   ) -> JSONTypes:
        '''Insert key with a value of default if key is not in the dictionary.

        Return the value for key if key is in the dictionary, else default.
        '''
        retriable_setdefault = functools.partial(
            self.__retriable_setdefault,
            dict_key,
            default=default,
        )
        self.__retry(retriable_setdefault)
        self._misses.discard(dict_key)
        if dict_key not in self or self[dict_key] is self._SENTINEL:
            value = self[dict_key] = default
        else:
            value = self[dict_key]
        return value

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    def __retriable_setdefault(self,
                               dict_key: JSONTypes,
                               default: JSONTypes = None,
                               ) -> None:
        with self._cache._watch() as pipeline:
            if dict_key not in self._cache:
                pipeline.multi()  # Available since Redis 1.2.0
                # The following line is equivalent to: self._cache[dict_key] = default
                pipeline.hset(  # Available since Redis 2.0.0
                    self._cache.key,
                    self._cache._encode(dict_key),
                    self._cache._encode(default),
                )

    def __retry(self, callable: Callable[[], Any], *, try_num: int = 0) -> Any:
        try:
            return callable()
        except WatchError:  # pragma: no cover
            if try_num < self._num_tries - 1:
                return self.__retry(callable, try_num=try_num+1)
            raise

    @_set_expiration
    def update(self,  # type: ignore
               arg: UpdateArg = tuple(),
               **kwargs: JSONTypes | object,
               ) -> None:
        '''D.update([E, ]**F) -> None.  Update D from dict/iterable E and F.
        If E is present and has an .items() method, then does:  for k in E: D[k] = E[k]
        If E is present and lacks an .items() method, then does:  for k, v in E: D[k] = v
        In either case, this is followed by: for k in F:  D[k] = F[k]

        The base class, OrderedDict, has an .update() method that works just
        fine.  The trouble is that it executes multiple calls to
        self.__setitem__() therefore multiple round trips to Redis.  This
        overridden .update() makes a single bulk call to Redis.
        '''
        to_cache = {}
        if isinstance(arg, collections.abc.Mapping):
            arg = arg.items()
        items = itertools.chain(arg, kwargs.items())
        for dict_key, value in items:
            if value is not self._SENTINEL:
                to_cache[dict_key] = value
                self._misses.discard(cast(JSONTypes, dict_key))
            super().__setitem__(dict_key, value)
        self._cache.update(to_cache)

    __update = update
