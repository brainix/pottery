# --------------------------------------------------------------------------- #
#   monkey.py                                                                 #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #
'Monkey patches.'


import logging


_logger = logging.getLogger('pottery')


# The Redis client doesn't have a sane equality test.  So monkey patch equality
# comparisons on to the Redis client.  We consider two Redis clients to be
# equal if they're connected to the same host, port, and database.

from redis import ConnectionPool  # isort:skip
from redis import Redis  # isort:skip

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
