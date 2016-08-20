#-----------------------------------------------------------------------------#
#   monkey.py                                                                 #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



def _connection(redis):
    keys = {'host', 'port', 'db'}
    dict_ = {key: redis.connection_pool.connection_kwargs[key] for key in keys}
    return dict_

def __eq__(self, other):
    if type(self) is not type(other):
        return False
    return _connection(self) == _connection(other)

def __ne__(self, other):
    return not __eq__(self, other)

from redis import Redis
Redis.__eq__ = __eq__
Redis.__ne__ = __ne__
