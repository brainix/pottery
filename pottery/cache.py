#-----------------------------------------------------------------------------#
#   cache.py                                                                  #
#                                                                             #
#   Copyright © 2015-2018, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections
import functools

from redis import Redis
from redis.exceptions import WatchError

from .dict import RedisDict



_DEFAULT_TIMEOUT = 365 * 24 * 60 * 60



def _arg_hash(*args, **kwargs):
    return hash((args, frozenset(kwargs.items())))



def redis_cache(*, key, redis=None, timeout=_DEFAULT_TIMEOUT):
    '''Redis-backed caching decorator.

    Arguments to the cached function must be hashable.

    Access the underlying function with f.__wrapped__.
    '''
    redis = Redis(socket_timeout=1) if redis is None else redis
    cache = RedisDict(redis=redis, key=key)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            hash_ = _arg_hash(*args, **kwargs)
            try:
                return_value = cache[hash_]
            except KeyError:
                return_value = func(*args, **kwargs)
                cache[hash_] = return_value
            redis.expire(key, timeout)
            return return_value
        wrapper.__wrapped__ = func
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
