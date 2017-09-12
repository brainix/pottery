#-----------------------------------------------------------------------------#
#   cache.py                                                                  #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import functools

from redis import Redis

from .dict import RedisDict



def _arg_hash(*args, **kwargs):
    return hash((args, frozenset(kwargs.items())))

def redis_cache(*, key, redis=None):
    redis = Redis() if redis is None else redis
    cache = RedisDict(redis=redis, key=key)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = _arg_hash(*args, **kwargs)
            try:
                return_value = cache[key]
            except KeyError:
                return_value = func(*args, **kwargs)
                cache[key] = return_value
            return return_value
        return wrapper
    return decorator
