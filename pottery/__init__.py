# --------------------------------------------------------------------------- #
#   __init__.py                                                               #
#                                                                             #
#   Copyright © 2015-2025, Rajiv Bakulesh Shah, original author.              #
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


from typing import Tuple

# TODO: When we drop support for Python 3.7, change the following import to:
#   from typing import Final
from typing_extensions import Final

from .counter import RedisCounter
from .deque import RedisDeque
from .dict import RedisDict
from .list import RedisList
from .queue import RedisSimpleQueue
from .set import RedisSet


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

from .cache import CachedOrderedDict  # isort:skip
from .cache import redis_cache  # isort:skip
from .aionextid import AIONextID  # isort:skip
from .nextid import NextID  # isort:skip
from .aioredlock import AIORedlock  # isort:skip
from .redlock import Redlock  # isort:skip
from .redlock import synchronize  # isort:skip
from .timer import ContextTimer  # isort:skip


from .bloom import BloomFilter  # isort:skip
from .hyper import HyperLogLog  # isort:skip


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

    'CachedOrderedDict',
    'redis_cache',
    'AIONextID',
    'NextID',
    'AIORedlock',
    'Redlock',
    'synchronize',
    'ContextTimer',

    'RedisCounter',
    'RedisDeque',
    'RedisDict',
    'RedisList',
    'RedisSimpleQueue',
    'RedisSet',
    'BloomFilter',
    'HyperLogLog',
)
