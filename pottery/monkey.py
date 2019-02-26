#-----------------------------------------------------------------------------#
#   monkey.py                                                                 #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#
'Monkey patches.'



import logging



_logger = logging.getLogger('pottery')



# Monkey patch os.listdir() to optionally return absolute paths.

import os

def _absolutize(*, files, path=None):
    path = os.path.abspath(path or '.')
    files = [os.path.join(path, f) for f in files]
    return files

def _listdir(path=None, *, absolute=False):
    files = _listdir.listdir(path)
    if absolute:    # pragma: no cover
        files = _absolutize(path=path, files=files)
    return files

_listdir.listdir = os.listdir
os.listdir = _listdir

_logger.info('Monkey patched os.listdir() to optionally return absolute paths')



# The Redis client doesn't have a sane equality test.  So monkey patch equality
# comparisons on to the Redis client.  We consider two Redis clients to be
# equal if they're connected to the same host, port, and database.

import collections
from redis import Redis

Connection = collections.namedtuple('Connection', ('host', 'port', 'db'))

def _connection(self):
    'An object representing a Redis connection (host, port, and database).'
    keys = {'host', 'port', 'db'}
    dict_ = {key: self.connection_pool.connection_kwargs[key] for key in keys}
    obj = Connection(**dict_)
    return obj

def __eq__(self, other):
    '''True if two Redis clients are equal.

    The Redis client doesn't have a sane equality test.  So we monkey patch
    this method on to the Redis client so that two client instances are equal
    if they're connected to the same Redis host, port, and database.
    '''
    equals = isinstance(other, Redis) and \
        self._connection() == other._connection()
    return equals

def __ne__(self, other):
    'True if two Redis clients are *not* equal.'
    does_not_equal = not self.__eq__(other)
    return does_not_equal

Redis._connection = _connection
Redis.__eq__ = __eq__
Redis.__ne__ = __ne__

_logger.info(
    'Monkey patched Redis._connection(), Redis.__eq__() and Redis.__ne__() to '
    'compare Redis clients by connection params'
)
