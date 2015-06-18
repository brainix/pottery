#-----------------------------------------------------------------------------#
#   monkey.py                                                                 #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from redis import Redis

def _connection(redis):
    keys = ('host', 'port', 'db')
    d = {key: redis.connection_pool.connection_kwargs[key] for key in keys}
    return d

def __eq__(self, other):
    return _connection(self) == _connection(other)

def __ne__(self, other):
    return not __eq__(self, other)

Redis.__eq__ = __eq__
Redis.__ne__ = __ne__
