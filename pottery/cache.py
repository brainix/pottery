#-----------------------------------------------------------------------------#
#   cache.py                                                                  #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections
import contextlib
import functools

from redis import Redis
from redis.exceptions import WatchError

from .dict import RedisDict



_DEFAULT_TIMEOUT = 365 * 24 * 60 * 60



CacheInfo = collections.namedtuple(
    'CacheInfo',
    ('hits', 'misses', 'maxsize', 'currsize'),
)
CacheInfo.__new__.__defaults__ = 0, 0, None, 0
CacheInfo.__doc__ = ''
with contextlib.suppress(AttributeError):
    CacheInfo.hits.__doc__ = ''
    CacheInfo.misses.__doc__ = ''
    CacheInfo.maxsize.__doc__ = ''
    CacheInfo.currsize.__doc__ = ''



def _arg_hash(*args, **kwargs):
    return hash((args, frozenset(kwargs.items())))



def redis_cache(*, key, redis=None, timeout=_DEFAULT_TIMEOUT):
    '''Redis-backed caching decorator.

    Arguments to the cached function must be hashable, and return values from
    the function must be JSON serializable.

    Access the underlying function with f.__wrapped__, bypass the cache (force
    a cache reset for your args/kwargs) with f.__bypass__, and clear/invalidate
    the entire cache with f.cache_clear.
    '''
    redis = Redis(socket_timeout=1) if redis is None else redis
    cache = RedisDict(redis=redis, key=key)
    hits, misses = 0, 0

    def decorator(func):
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
            return CacheInfo(
                hits=hits,
                misses=misses,
                maxsize=None,
                currsize=len(cache),
            )

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
