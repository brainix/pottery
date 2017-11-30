#-----------------------------------------------------------------------------#
#   cache.py                                                                  #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import functools

from redis import Redis

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
