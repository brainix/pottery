#-----------------------------------------------------------------------------#
#   monkey.py                                                                 #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#
'Monkey patches.'



from redis import Redis

@property
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
    equals = isinstance(other, Redis) and self._connection == other._connection
    return equals

def __ne__(self, other):
    'True if two Redis clients are *not* equal.'
    does_not_equal = not self.__eq__(other)
    return does_not_equal

Redis._connection = _connection
Redis.__eq__ = __eq__
Redis.__ne__ = __ne__
