# --------------------------------------------------------------------------- #
#   nextid.py                                                                 #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
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
'''Distributed Redis-powered monotonically increasing ID generator.

Rationale and algorithm description:
    http://antirez.com/news/102

Lua scripting:
    https://github.com/andymccurdy/redis-py#lua-scripting
'''


import concurrent.futures
import contextlib
import logging
from typing import ClassVar
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import cast

from redis import Redis
from redis import RedisError
from redis.client import Script
from typing_extensions import Final

from .base import Primitive
from .exceptions import QuorumIsImpossible
from .exceptions import QuorumNotAchieved
from .executor import BailOutExecutor


_logger: Final[logging.Logger] = logging.getLogger('pottery')


class _Scripts:
    '''Parent class to define/register Lua scripts for Redis.

    Note that we only have to register these Lua scripts once -- so we do it on
    the first instantiation of NextId.
    '''

    __slots__: Tuple[str, ...] = tuple()

    _set_id_script: ClassVar[Optional[Script]] = None

    def __init__(self,
                 *,
                 key: str = 'current',
                 masters: Iterable[Redis] = frozenset(),
                 raise_on_redis_errors: bool = False,
                 ) -> None:
        super().__init__(  # type: ignore
            key=key,
            masters=masters,
            raise_on_redis_errors=raise_on_redis_errors,
        )
        self.__register_set_id_script()

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    def __register_set_id_script(self) -> None:
        if self._set_id_script is None:
            class_name = self.__class__.__qualname__
            _logger.info('Registering %s._set_id_script', class_name)
            master = next(iter(self.masters))  # type: ignore
            self.__class__._set_id_script = master.register_script('''
                local curr = tonumber(redis.call('get', KEYS[1]))
                local next = tonumber(ARGV[1])
                if curr == nil or curr < next then
                    redis.call('set', KEYS[1], next)
                    return next
                else
                    return nil
                end
            ''')


class NextId(_Scripts, Primitive):
    '''Distributed Redis-powered monotonically increasing ID generator.

    This algorithm safely and reliably produces monotonically increasing IDs
    across threads, processes, and even machines, without a single point of
    failure.  Two caveats:

        1.  If many clients are generating IDs concurrently, then there may be
            "holes" in the sequence of IDs (e.g.: 1, 2, 6, 10, 11, 21, ...).

        2.  This algorithm scales to about 5,000 IDs per second (with 5 Redis
            masters).  If you need IDs faster than that, then you may want to
            consider other techniques.

    Rationale and algorithm description:
        http://antirez.com/news/102

    Clean up Redis for the doctest:

        >>> from redis import Redis
        >>> redis = Redis(socket_timeout=1)
        >>> redis.delete('nextid:tweet-ids') in {0, 1}
        True

    Usage:

        >>> tweet_ids_1 = NextId(key='tweet-ids', masters={redis})
        >>> tweet_ids_2 = NextId(key='tweet-ids', masters={redis})
        >>> next(tweet_ids_1)
        1
        >>> next(tweet_ids_2)
        2
        >>> next(tweet_ids_1)
        3
        >>> tweet_ids_1.reset()
        >>> next(tweet_ids_1)
        1
    '''

    __slots__ = ('num_tries',)

    KEY_PREFIX: ClassVar[str] = 'nextid'
    NUM_TRIES: ClassVar[int] = 3

    def __init__(self,
                 *,
                 key: str = 'current',
                 masters: Iterable[Redis] = frozenset(),
                 raise_on_redis_errors: bool = False,
                 num_tries: int = NUM_TRIES,
                 ) -> None:
        super().__init__(
            key=key,
            masters=masters,
            raise_on_redis_errors=raise_on_redis_errors,
        )
        self.num_tries = num_tries

    def __iter__(self) -> 'NextId':
        return self

    def __next__(self) -> int:
        suppressable_errors: List[Type[BaseException]] = [QuorumNotAchieved]
        if not self.raise_on_redis_errors:
            suppressable_errors.append(QuorumIsImpossible)
        for _ in range(self.num_tries):
            with contextlib.suppress(*suppressable_errors):
                next_id = self.__current_id + 1
                self.__current_id = next_id
                return next_id
        raise QuorumNotAchieved(self.key, self.masters)

    @property
    def __current_id(self) -> int:
        with BailOutExecutor() as executor:
            futures = set()
            for master in self.masters:
                future = executor.submit(master.get, self.key)
                futures.add(future)

            current_ids, redis_errors = [], []
            for future in concurrent.futures.as_completed(futures):
                try:
                    current_id = int(cast(bytes, future.result() or b'0'))
                except RedisError as error:
                    redis_errors.append(error)
                    _logger.exception(
                        '%s.__current_id() getter caught %s',
                        self.__class__.__name__,
                        error.__class__.__name__,
                    )
                else:
                    current_ids.append(current_id)
                    if len(current_ids) > len(self.masters) // 2:  # pragma: no cover
                        return max(current_ids)

        self._check_enough_masters_up(None, redis_errors)
        raise QuorumNotAchieved(
            self.key,
            self.masters,
            redis_errors=redis_errors,
        )

    @__current_id.setter
    def __current_id(self, value: int) -> None:
        with BailOutExecutor() as executor:
            futures = set()
            for master in self.masters:
                future = executor.submit(
                    cast(Script, self._set_id_script),
                    keys=(self.key,),
                    args=(value,),
                    client=master,
                )
                futures.add(future)

            num_masters_set, redis_errors = 0, []
            for future in concurrent.futures.as_completed(futures):
                try:
                    num_masters_set += future.result() == value
                except RedisError as error:
                    redis_errors.append(error)
                    _logger.exception(
                        '%s.__current_id() setter caught %s',
                        self.__class__.__name__,
                        error.__class__.__name__,
                    )
                else:
                    if num_masters_set > len(self.masters) // 2:  # pragma: no cover
                        return

        self._check_enough_masters_up(None, redis_errors)
        raise QuorumNotAchieved(
            self.key,
            self.masters,
            redis_errors=redis_errors,
        )

    def reset(self) -> None:
        'Reset the ID counter to 0.'
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = set()
            for master in self.masters:
                future = executor.submit(master.delete, self.key)
                futures.add(future)

            num_masters_reset, redis_errors = 0, []
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except RedisError as error:
                    redis_errors.append(error)
                    _logger.exception(
                        '%s.reset() caught %s',
                        self.__class__.__name__,
                        error.__class__.__name__,
                    )
                else:
                    num_masters_reset += 1
                    if num_masters_reset == len(self.masters):  # pragma: no cover
                        return

        self._check_enough_masters_up(None, redis_errors)
        raise QuorumNotAchieved(
            self.key,
            self.masters,
            redis_errors=redis_errors,
        )

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} key={self.key}>'


if __name__ == '__main__':
    # Run the doctests in this module with:
    #   $ source venv/bin/activate
    #   $ python3 -m pottery.nextid
    #   $ deactivate
    with contextlib.suppress(ImportError):
        from tests.base import run_doctests
        run_doctests()
