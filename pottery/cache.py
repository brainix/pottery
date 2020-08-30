# --------------------------------------------------------------------------- #
#   cache.py                                                                  #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import collections
import contextlib
import functools
import itertools
import logging
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import FrozenSet
from typing import Hashable
from typing import NamedTuple
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import cast

from redis import Redis
from redis.exceptions import WatchError
from typing_extensions import Final
from typing_extensions import final

from .base import JSONTypes
from .base import _default_redis
from .base import random_key
from .dict import InitArg
from .dict import InitIter
from .dict import InitMap
from .dict import RedisDict


_DEFAULT_TIMEOUT: Final[int] = 60   # seconds

_logger: Final[logging.Logger] = logging.getLogger('pottery')


class CacheInfo(NamedTuple):
    hits: int = 0
    misses: int = 0
    maxsize: Optional[int] = None
    currsize: int = 0


def _arg_hash(*args: Hashable, **kwargs: Hashable) -> int:
    return hash((args, frozenset(kwargs.items())))


F = TypeVar('F', bound=Callable[..., JSONTypes])


def redis_cache(*,
                redis: Optional[Redis] = None,
                key: Optional[str] = None,
                timeout: Optional[int] = _DEFAULT_TIMEOUT,
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

    redis = _default_redis if redis is None else redis

    def decorator(func: F) -> F:
        nonlocal redis, key
        if key is None:  # pragma: no cover
            key = random_key(redis=cast(Redis, redis))
            _logger.info(
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
            cast(Redis, redis).delete(cast(str, key))
            hits, misses = 0, 0

        wrapper.__wrapped__ = func  # type: ignore
        wrapper.__bypass__ = bypass  # type: ignore
        wrapper.cache_info = cache_info  # type: ignore
        wrapper.cache_clear = cache_clear  # type: ignore
        return cast(F, wrapper)
    return decorator


@final
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

    Properties 1 and 2 are satisfied by Python's OrderedDict.  However,
    CachedOrderedDict extends Python's OrderedDict to also satisfy property 3.
    '''

    _SENTINEL: ClassVar[object] = object()
    _NUM_TRIES: ClassVar[int] = 3

    def __init__(self,
                 *,
                 key: Optional[str] = None,
                 redis: Optional[Redis] = None,
                 keys: Tuple[JSONTypes, ...] = tuple(),
                 num_tries: int = _NUM_TRIES,
                 ) -> None:
        self._num_tries = num_tries
        redis = _default_redis if redis is None else redis
        init_cache = functools.partial(RedisDict, redis=redis, key=key)
        self._cache = self._retry(init_cache)
        self._misses = set()

        # We have to iterate over keys multiple times, so cast it to a tuple.
        # This allows the caller to pass in a generator for keys, and we can
        # still iterate over it multiple times.
        items, keys = [], tuple(keys)
        if keys:
            encoded_keys = (self._cache._encode(key_) for key_ in keys)
            encoded_values = redis.hmget(self._cache.key, *encoded_keys)
            for key_, encoded_value in zip(keys, encoded_values):
                if encoded_value is None:
                    value = self._SENTINEL
                    self._misses.add(key_)
                else:
                    value = self._cache._decode(encoded_value)
                item = (key_, value)
                items.append(item)
        return super().__init__(items)

    def misses(self) -> FrozenSet[JSONTypes]:
        return frozenset(self._misses)

    def __setitem__(self, key: JSONTypes, value: JSONTypes) -> None:
        if value is not self._SENTINEL:
            self._cache[key] = value
            self._misses.discard(key)
        return super().__setitem__(key, value)

    def setdefault(self,
                   key: JSONTypes,
                   default: JSONTypes = None,
                   ) -> JSONTypes:
        retriable_setdefault = functools.partial(
            self._retriable_setdefault,
            key,
            default=default,
        )
        self._retry(retriable_setdefault)
        self._misses.discard(key)
        if key not in self or self[key] is self._SENTINEL:
            self[key] = default
        return cast(JSONTypes, self[key])

    def _retriable_setdefault(self,
                              key: JSONTypes,
                              default: JSONTypes = None,
                              ) -> None:
        with self._cache._watch():
            if key not in self._cache:
                self._cache.redis.multi()
                self._cache[key] = default

    def _retry(self, callable: Callable[[], Any], try_num: int = 0) -> Any:
        try:
            return callable()
        except WatchError:  # pragma: no cover
            if try_num < self._num_tries - 1:
                return self._retry(callable, try_num=try_num+1)
            else:
                raise

    def update(self, arg: InitArg = tuple(), **kwargs: JSONTypes) -> None:  # type: ignore
        to_cache, to_set = {}, {}
        with contextlib.suppress(AttributeError):
            arg = cast(InitMap, arg).items()
        for key, value in itertools.chain(cast(InitIter, arg), kwargs.items()):
            if value is not self._SENTINEL:
                to_cache[key] = value
                self._misses.discard(key)
            to_set[key] = value
        self._cache.update(to_cache)
        for key, value in to_set.items():
            super().__setitem__(key, value)
