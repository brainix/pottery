# --------------------------------------------------------------------------- #
#   nextid.py                                                                 #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
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
from typing import Optional
from typing import cast

from redis import Redis
from redis import RedisError
from redis.client import Script
from typing_extensions import Final

from .base import Primitive
from .exceptions import QuorumNotAchieved
from .executor import BailOutExecutor


_logger: Final[logging.Logger] = logging.getLogger('pottery')


class NextId(Primitive):
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
        >>> Redis(socket_timeout=1).delete('nextid:current') in {0, 1}
        True

    Usage:

        >>> ids1 = NextId()
        >>> ids2 = NextId()
        >>> next(ids1)
        1
        >>> next(ids2)
        2
        >>> next(ids1)
        3
    '''

    KEY_PREFIX: ClassVar[str] = 'nextid'
    KEY: ClassVar[str] = 'current'
    NUM_TRIES: ClassVar[int] = 3

    _set_id_script: ClassVar[Optional[Script]] = None

    def __init__(self,
                 *,
                 key: str = KEY,
                 masters: Iterable[Redis] = frozenset(),
                 num_tries: int = NUM_TRIES,
                 ) -> None:
        super().__init__(key=key, masters=masters)
        self.__register_set_id_script()
        self.num_tries = num_tries
        self.__init_masters()

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    def __register_set_id_script(self) -> None:
        if self._set_id_script is None:
            _logger.info(
                'Registering %s._set_id_script',
                self.__class__.__name__,
            )
            master = next(iter(self.masters))
            self.__class__._set_id_script = master.register_script('''
                local curr = tonumber(redis.call('get', KEYS[1]))
                local next = tonumber(ARGV[1])
                if curr < next then
                    redis.call('set', KEYS[1], next)
                    return next
                else
                    return nil
                end
            ''')

    def __init_masters(self) -> None:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for master in self.masters:
                executor.submit(master.setnx, self.key, 0)

    def __iter__(self) -> 'NextId':
        return self

    def __next__(self) -> int:
        for _ in range(self.num_tries):
            with contextlib.suppress(QuorumNotAchieved):
                next_id = self.__current_id + 1
                self.__current_id = next_id
                return next_id
        else:
            raise QuorumNotAchieved(self.masters, self.key)

    def __repr__(self) -> str:
        return (
            f'<{self.__class__.__name__} key={self.key} '
            f'value={self.__current_id}>'
        )

    @property
    def __current_id(self) -> int:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = set()
            for master in self.masters:
                future = executor.submit(master.get, self.key)
                futures.add(future)

            current_ids = []
            for future in concurrent.futures.as_completed(futures):
                try:
                    current_id = int(future.result())
                except RedisError as error:
                    _logger.exception(
                        '%s.__current_id() getter caught an %s',
                        self.__class__.__name__,
                        error.__class__.__name__,
                    )
                else:
                    current_ids.append(current_id)

        num_masters_gotten = len(current_ids)
        quorum = num_masters_gotten >= len(self.masters) // 2 + 1
        if quorum:
            return max(current_ids)
        else:
            raise QuorumNotAchieved(self.masters, self.key)

    @__current_id.setter
    def __current_id(self, value: int) -> None:
        quorum = False

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

            num_masters_set = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    num_masters_set += future.result() == value
                except RedisError as error:
                    _logger.exception(
                        '%s.__current_id() setter caught an %s',
                        self.__class__.__name__,
                        error.__class__.__name__,
                    )
                else:
                    quorum = num_masters_set >= len(self.masters) // 2 + 1
                    if quorum:  # pragma: no cover
                        break

        if not quorum:
            raise QuorumNotAchieved(self.masters, self.key)


if __name__ == '__main__':  # pragma: no cover
    # Run the doctests in this module with:
    #   $ source venv/bin/activate
    #   $ python3 -m pottery.nextid
    #   $ deactivate
    with contextlib.suppress(ImportError):
        from tests.base import run_doctests  # type: ignore
        run_doctests()
