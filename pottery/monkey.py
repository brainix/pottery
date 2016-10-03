#-----------------------------------------------------------------------------#
#   monkey.py                                                                 #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from redis import Redis

@property
def _connection(self):
    keys = {'host', 'port', 'db'}
    dict_ = {key: self.connection_pool.connection_kwargs[key] for key in keys}
    return dict_

def __eq__(self, other):
    equals = isinstance(other, Redis) and self._connection == other._connection
    return equals

def __ne__(self, other):
    does_not_equal = not self.__eq__(other)
    return does_not_equal

Redis._connection = _connection
Redis.__eq__ = __eq__
Redis.__ne__ = __ne__
