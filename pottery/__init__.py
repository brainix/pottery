# --------------------------------------------------------------------------- #
#   __init__.py                                                               #
#                                                                             #
#   Copyright © 2015-2022, Rajiv Bakulesh Shah, original author.              #
#                                                                             #
#   Licensed under the Apache License, Version 2.0 (the "License");           #
#   you may not use this file except in compliance with the License.          #
#   You may obtain a copy of the License at:                                  #
#       http://www.apache.org/licenses/LICENSE-2.0                            #
#                                                                             #
#   Unless required by applicable law or agreed to in writing, software       #
#   distributed under the License is distributed on an "AS IS" BASIS,         #
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  #
#   See the License for the specific language governing permissions and       #
#   limitations under the License.                                            #
# --------------------------------------------------------------------------- #
'''Redis for Humans.

Redis is awesome, but Redis commands are not always intuitive.  Pottery is a
Pythonic way to access Redis.  If you know how to use Python dicts, then you
already know how to use Pottery.  Pottery is useful for accessing Redis more
easily, and also for implementing microservice resilience patterns; and it has
been battle tested in production at scale.
'''


from typing import Tuple

# TODO: When we drop support for Python 3.7, change the following import to:
#   from typing import Final
from typing_extensions import Final


__title__: Final[str] = 'pottery'
__version__: Final[str] = '3.0.0'
__description__: Final[str] = __doc__.split(sep='\n\n', maxsplit=1)[0]
__url__: Final[str] = 'https://github.com/brainix/pottery'
__author__: Final[str] = 'Rajiv Bakulesh Shah'
__author_email__: Final[str] = 'brainix@gmail.com'
__license__: Final[str] = 'Apache 2.0'
__keywords__: Final[str] = 'Redis client persistent storage'
__copyright__: Final[str] = f'Copyright © 2015-2022, {__author__}, original author.'


from .monkey import PotteryEncoder  # isort: skip

from .exceptions import PotteryError  # isort:skip
from .exceptions import KeyExistsError  # isort:skip
from .exceptions import RandomKeyError  # isort:skip
from .exceptions import QueueEmptyError  # isort:skip
from .exceptions import PrimitiveError  # isort:skip
from .exceptions import QuorumIsImpossible  # isort:skip
from .exceptions import QuorumNotAchieved  # isort:skip
from .exceptions import TooManyExtensions  # isort:skip
from .exceptions import ExtendUnlockedLock  # isort:skip
from .exceptions import ReleaseUnlockedLock  # isort:skip
from .exceptions import PotteryWarning  # isort:skip
from .exceptions import InefficientAccessWarning  # isort:skip

from .bloom import BloomFilter  # isort:skip
from .cache import CachedOrderedDict  # isort:skip
from .cache import redis_cache  # isort:skip
from .hyper import HyperLogLog  # isort:skip
from .nextid import NextId  # isort:skip
from .redlock import Redlock  # isort:skip
from .redlock import synchronize  # isort:skip
from .timer import ContextTimer  # isort:skip

from .counter import RedisCounter
from .deque import RedisDeque
from .dict import RedisDict
from .list import RedisList
from .queue import RedisSimpleQueue
from .set import RedisSet


__all__: Final[Tuple[str, ...]] = (
    'PotteryEncoder',

    'PotteryError',
    'KeyExistsError',
    'RandomKeyError',
    'QueueEmptyError',
    'PrimitiveError',
    'QuorumIsImpossible',
    'QuorumNotAchieved',
    'TooManyExtensions',
    'ExtendUnlockedLock',
    'ReleaseUnlockedLock',
    'PotteryWarning',
    'InefficientAccessWarning',

    'BloomFilter',
    'CachedOrderedDict',
    'redis_cache',
    'HyperLogLog',
    'NextId',
    'Redlock',
    'synchronize',
    'ContextTimer',

    'RedisCounter',
    'RedisDeque',
    'RedisDict',
    'RedisList',
    'RedisSimpleQueue',
    'RedisSet',
)
