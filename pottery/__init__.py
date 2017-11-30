#-----------------------------------------------------------------------------#
#   __init__.py                                                               #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#
'''Redis for Humans.

Redis is awesome, but Redis commands are not always fun.  Pottery is a Pythonic
way to access Redis.  If you know how to use Python dicts, then you already
know how to use Pottery.
'''



__title__ = 'pottery'
__version__ = '0.46'
__description__, __long_description__ = (
    s.strip() for s in __doc__.split('\n\n', 1)
)
__url__ = 'https://github.com/brainix/pottery'
__author__ = 'Rajiv Bakulesh Shah'
__author_email__ = 'brainix@gmail.com'
__license__ = 'Apache 2.0'
__keywords__ = 'Redis client persistent storage'
__copyright__ = 'Copyright © 2015-2017, {}, original author.'.format(__author__)



from .exceptions import PotteryError
from .exceptions import KeyExistsError
from .exceptions import RandomKeyError
from .exceptions import PrimitiveError
from .exceptions import QuorumNotAchieved
from .exceptions import TooManyExtensions
from .exceptions import ReleaseUnlockedLock

from .cache import redis_cache
from .contexttimer import ContextTimer
from .nextid import NextId
from .redlock import Redlock

from .counter import RedisCounter
from .deque import RedisDeque
from .dict import RedisDict
from .list import RedisList
from .set import RedisSet
