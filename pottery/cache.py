#-----------------------------------------------------------------------------#
#   cache.py                                                                  #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections
import functools
import logging

from redis import Redis
from redis.exceptions import WatchError

from .base import random_key
from .dict import RedisDict



_DEFAULT_TIMEOUT = 60   # seconds



_logger = logging.getLogger('pottery')



CacheInfo = collections.namedtuple(
    'CacheInfo',
    ('hits', 'misses', 'maxsize', 'currsize'),
)
CacheInfo.__new__.__defaults__ = 0, 0, None, 0
CacheInfo.__doc__ = ''



def _arg_hash(*args, **kwargs):
    return hash((args, frozenset(kwargs.items())))



def redis_cache(*, redis=None, key=None, timeout=_DEFAULT_TIMEOUT):
    '''Redis-backed caching decorator with an API like functools.lru_cache().

    Arguments to the original underlying function must be hashable, and return
    values from the function must be JSON serializable.

    Additionally, this decorator provides the following functions:

    f.__wrapped__(*args, **kwargs)
        Provide access to the original underlying function.  This is useful for
        introspection, for bypassing the cache, or for rewrapping the function
        with a different cache.

    f.__bypass__(*args, **kwargs)
        Force a cache reset for your args/kwargs.  Bypass the cache lookup,
        call the original underlying function, then cache the results for
        future calls to f(*args, **kwargs).

    f.cache_info()
        Return a namedtuple showing hits, misses, maxsize, and currsize.  This
        information is helpful for measuring the effectiveness of the cache.
        Note that maxsize is always None, meaning that this cache is always
        unbounded.  maxsize is only included for compatibility with
        functools.lru_cache().

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
    def decorator(func):
        nonlocal redis, key
        redis = Redis(socket_timeout=1) if redis is None else redis
        if key is None:                             # pragma: no cover
            key = random_key(redis=redis)
            _logger.info(
                "Self-assigning key redis_cache(key='%s') for function %s",
                key,
                func.__qualname__,
            )
        cache = RedisDict(redis=redis, key=key)
        hits, misses = 0, 0

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal hits, misses
            hash_ = _arg_hash(*args, **kwargs)
            try:
                return_value = cache[hash_]
                hits += 1
            except KeyError:
                return_value = func(*args, **kwargs)
                cache[hash_] = return_value
                misses += 1
            redis.expire(key, timeout)
            return return_value

        @functools.wraps(func)
        def bypass(*args, **kwargs):
            hash_ = _arg_hash(*args, **kwargs)
            return_value = func(*args, **kwargs)
            cache[hash_] = return_value
            redis.expire(key, timeout)
            return return_value

        def cache_info():
            return CacheInfo(hits=hits, misses=misses, currsize=len(cache))

        def cache_clear():
            nonlocal hits, misses
            redis.delete(key)
            hits, misses = 0, 0

        wrapper.__wrapped__ = func
        wrapper.__bypass__ = bypass
        wrapper.cache_info = cache_info
        wrapper.cache_clear = cache_clear
        return wrapper
    return decorator



class CachedOrderedDict(collections.OrderedDict):
    _SENTINEL = object()
    _NUM_RETRIES = 3

    def __init__(self, *, redis=None, key=None, keys=tuple(),
                 num_retries=_NUM_RETRIES):
        self._num_retries = num_retries
        partial = functools.partial(RedisDict, redis=redis, key=key)
        self._cache = self._retry(partial)
        self.misses = set()

        items = []
        for key_ in keys:
            try:
                item = (key_, self._cache[key_])
            except KeyError:
                self.misses.add(key_)
                item = (key_, self._SENTINEL)
            items.append(item)
        super().__init__(items)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:                        # pragma: no cover
            cache = {key_: self[key_] for key_ in self.misses}
            partial = functools.partial(self._cache.update, cache)
            self._retry(partial)

    def _retry(self, partial):
        for retry_num in range(self._num_retries):  # pragma: no cover
            try:
                return partial()
            except WatchError:
                if retry_num == self._num_retries - 1:
                    raise
