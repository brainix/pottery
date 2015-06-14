#-----------------------------------------------------------------------------#
#   exceptions.py                                                             #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



class _PotteryError(Exception):
    def __init__(self, redis, key):
        self._redis = redis
        self._key = key

    def __str__(self):
        return str((self._redis, self._key))



class KeyExistsError(_PotteryError):
    """Initializing a container on a Redis key that already exists."""

    def __str__(self):
        return self._key
