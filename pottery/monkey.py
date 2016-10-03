#-----------------------------------------------------------------------------#
#   monkey.py                                                                 #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from redis import Redis

def _connection(redis):
    keys = {'host', 'port', 'db'}
    dict_ = {key: redis.connection_pool.connection_kwargs[key] for key in keys}
    return dict_

def __eq__(self, other):
    return isinstance(other, Redis) and _connection(self) == _connection(other)

def __ne__(self, other):
    return not __eq__(self, other)

Redis.__eq__ = __eq__
Redis.__ne__ = __ne__
