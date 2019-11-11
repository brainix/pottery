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
    if absolute:            # pragma: no cover
        files = _absolutize(path=path, files=files)
    return files

_listdir.listdir = os.listdir
os.listdir = _listdir

_logger.info('Monkey patched os.listdir() to optionally return absolute paths')



# The Redis client doesn't have a sane equality test.  So monkey patch equality
# comparisons on to the Redis client.  We consider two Redis clients to be
# equal if they're connected to the same host, port, and database.
#
# I've submitted this change to redis-py:
#     https://github.com/andymccurdy/redis-py/pull/1240
#
# If it gets merged upstream, then I'll be able to delete this monkey patch.

from redis import ConnectionPool
from redis import Redis

def __eq__(self, other):
    try:
        return self.connection_kwargs == other.connection_kwargs
    except AttributeError:  # pragma: no cover
        return False

ConnectionPool.__eq__ = __eq__

def __eq__(self, other):
    '''True if two Redis clients are equal.

    The Redis client doesn't have a sane equality test.  So we monkey patch
    this method on to the Redis client so that two client instances are equal
    if they're connected to the same Redis host, port, and database.
    '''
    try:
        return self.connection_pool == other.connection_pool
    except AttributeError:
        return False

Redis.__eq__ = __eq__

_logger.info(
    'Monkey patched ConnectionPool.__eq__() and Redis.__eq__() to compare '
    'Redis clients by connection params'
)
