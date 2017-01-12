#-----------------------------------------------------------------------------#
#   monkey.py                                                                 #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#
'Monkey patches.'



# Monkey patch os.listdir() to optionally return absolute paths.

import os

def _absolutize(*, files, path=None):
    path = os.path.abspath(path or '.')
    files = [os.path.join(path, f) for f in files]
    return files

def _listdir(path=None, *, absolute=False):
    files = _listdir.listdir(path)
    if absolute:
        files = _absolutize(path=path, files=files)
    return files

_listdir.listdir = os.listdir
os.listdir = _listdir



# Monkey patch equality comparisons on to the Redis client.  We consider two
# Redis clients to be equal if they're connected to the same host, port, and
# database.

from redis import Redis

def _connection(self):
    'A dictionary representing a Redis connection (host, port, and database).'
    keys = {'host', 'port', 'db'}
    dict_ = {key: self.connection_pool.connection_kwargs[key] for key in keys}
    return dict_

def __eq__(self, other):
    '''True if two Redis clients are equal.

    The Redis client doesn't have a sane equality test.  So we monkey patch
    this method on to the Redis client so that two client instances are equal
    if they're connected to the same Redis host, port, and database.
    '''
    equals = isinstance(other, Redis) and self._connection() == other._connection()
    return equals

def __ne__(self, other):
    'True if two Redis clients are *not* equal.'
    does_not_equal = not self.__eq__(other)
    return does_not_equal

Redis._connection = _connection
Redis.__eq__ = __eq__
Redis.__ne__ = __ne__



# Monkey patch isort.SortImports to have a correctly_sorted property.  Compare:
#   >>> assert not SortImports('monkey.py').incorrectly_sorted
#   >>> assert SortImports('monkey.py').correctly_sorted

@property
def _correctly_sorted(self):
    return not self.incorrectly_sorted

import contextlib
with contextlib.suppress(ImportError):
    from isort import SortImports
    SortImports.correctly_sorted = _correctly_sorted
