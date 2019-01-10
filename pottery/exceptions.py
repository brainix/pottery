#-----------------------------------------------------------------------------#
#   exceptions.py                                                             #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



class PotteryError(Exception):
    'Base exception class for Pottery containers.'

    def __init__(self, redis, key):
        super().__init__()
        self._redis = redis
        self._key = key

    def __repr__(self):
        return "{}(redis={}, key='{}')".format(
            self.__class__.__name__,
            self._redis,
            self._key,
        )

    def __str__(self):
        return "redis={} key='{}'".format(self._redis, self._key)

class KeyExistsError(PotteryError):
    'Initializing a container on a Redis key that already exists.'

class RandomKeyError(PotteryError, RuntimeError):
    "Can't create a random Redis key; all of our attempts already exist."

    def __init__(self, redis):
        super().__init__(redis, None)

    def __repr__(self):
        return "{}(redis={})".format(self.__class__.__name__, self._redis)

    def __str__(self):
        return 'redis={}'.format(self._redis)



class PrimitiveError(Exception):
    'Base exception class for distributed primitives.'

    def __init__(self, masters, key):
        super().__init__()
        self._masters = masters
        self._key = key

    def __repr__(self):
        return "{}(masters={}, key='{}')".format(
            self.__class__.__name__,
            list(self._masters),
            self._key,
        )

    def __str__(self):
        return "masters={}, key='{}'".format(list(self._masters), self._key)

class QuorumNotAchieved(PrimitiveError, RuntimeError):
    ...

class TooManyExtensions(PrimitiveError, RuntimeError):
    ...

class ReleaseUnlockedLock(PrimitiveError, RuntimeError):
    ...
