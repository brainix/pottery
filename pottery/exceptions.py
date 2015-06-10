#-----------------------------------------------------------------------------#
#   exceptions.py                                                             #
#                                                                             #
#   Copyright (c) 2015-2016, Rajiv Bakulesh Shah.                             #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



class PotteryError(Exception):
    'Base exception class.'

    def __init__(self, redis, key):
        self._redis = redis
        self._key = key

    def __str__(self):
        return str((self._redis, self._key))



class KeyExistsError(PotteryError):
    'Initializing a container on a Redis key that already exists.'

    def __str__(self):
        return self._key



class RandomKeyError(PotteryError):
    "Can't create a random Redis key; all of our attempts already exist."

    def __str__(self):
        return str(self._redis)



class TooManyTriesError(PotteryError):
    "Can't complete a Redis transaction; tried too many times."
