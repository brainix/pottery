#-----------------------------------------------------------------------------#
#   __init__.py                                                               #
#                                                                             #
#   Copyright (c) 2015-2016, Rajiv Bakulesh Shah.                             #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#
'''Redis for Humans.

Redis is awesome, but using Redis commands is not always fun.  Pottery is a
Pythonic way to access Redis.  If you know how to use Python dicts and sets,
then you already know how to use Pottery.
'''

__title__ = 'pottery'
__version__ = '0.29'
__description__, __long_description__ = (s.strip() for s in __doc__.split('\n\n', 1))
__url__ = 'https://github.com/brainix/pottery'
__author__ = 'Rajiv Bakulesh Shah'
__author_email__ = 'brainix@gmail.com'
__license__ = 'Apache 2.0'
__keywords__ = 'Redis client persistent storage'
__copyright__ = 'Copyright (c) 2015-2016, {}'.format(__author__)

from .exceptions import PotteryError
from .exceptions import KeyExistsError
from .exceptions import RandomKeyError
from .exceptions import TooManyTriesError

from .counter import RedisCounter
from .deque import RedisDeque
from .dict import RedisDict
from .list import RedisList
from .set import RedisSet
