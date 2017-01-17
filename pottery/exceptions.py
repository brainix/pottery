#-----------------------------------------------------------------------------#
#   exceptions.py                                                             #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



class PotteryError(Exception):
    'Base exception class.'

    def __init__(self, redis, key):
        self._redis = redis
        self._key = key

    def __str__(self):  # pragma: no cover
        return str((self._redis, self._key))



class KeyExistsError(PotteryError):
    'Initializing a container on a Redis key that already exists.'

    def __str__(self):
        return self._key



class RandomKeyError(PotteryError):
    "Can't create a random Redis key; all of our attempts already exist."

    def __str__(self):
        return str(self._redis)
