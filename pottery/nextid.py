#-----------------------------------------------------------------------------#
#   nextid.py                                                                 #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#
'''Distributed Redis-powered monotonically increasing ID generator.

Rationale and algorithm description:
    http://antirez.com/news/102

Lua scripting:
    https://github.com/andymccurdy/redis-py#lua-scripting
'''



import concurrent.futures
import contextlib

from redis import Redis
from redis.exceptions import ConnectionError
from redis.exceptions import TimeoutError

from .base import Primitive



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

        >>> Redis().delete('nextid:current') in {0, 1}
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

    KEY_PREFIX = 'nextid'
    KEY = 'current'
    NUM_TRIES = 3

    def __init__(self, *, key=KEY, num_tries=NUM_TRIES, masters=frozenset()):
        super().__init__(key=key, masters=masters)
        self.num_tries = num_tries
        self._set_id_script = self._register_set_id_script()
        self._init_masters()

    def _register_set_id_script(self):
        master = next(iter(self.masters))
        set_id_script = master.register_script('''
            local curr = tonumber(redis.call('get', KEYS[1]))
            local next = tonumber(ARGV[1])
            if curr < next then
                redis.call('set', KEYS[1], next)
                return next
            else
                return nil
            end
        ''')
        return set_id_script

    def _init_masters(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.masters)) as executor:
            for master in self.masters:
                executor.submit(master.setnx, self.key, 0)

    def __iter__(self):
        return self

    def __next__(self):
        for _ in range(self.num_tries):
            with contextlib.suppress(RuntimeError):
                next_id = self._current_id + 1
                self._current_id = next_id
                return next_id
        else:
            raise RuntimeError('quorum not achieved')

    def __repr__(self):
        return '<{} key={} value={}>'.format(
            self.__class__.__name__,
            self.key,
            self._current_id,
        )

    @property
    def _current_id(self):
        current_id, num_masters = 0, 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.masters)) as executor:
            futures = {executor.submit(master.get, self.key) for master in self.masters}
            for future in concurrent.futures.as_completed(futures):
                with contextlib.suppress(TimeoutError, ConnectionError):
                    current_id = max(current_id, int(future.result()))
                    num_masters += 1
        if num_masters < len(self.masters) // 2 + 1:
            raise RuntimeError('quorum not achieved')
        else:
            return current_id

    @_current_id.setter
    def _current_id(self, value):
        num_masters = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.masters)) as executor:
            futures = {executor.submit(self._set_id_script, keys=(self.key,), args=(value,), client=master) for master in self.masters}
            for future in concurrent.futures.as_completed(futures):
                with contextlib.suppress(TimeoutError, ConnectionError):
                    num_masters += future.result() == value
        if num_masters < len(self.masters) // 2 + 1:
            raise RuntimeError('quorum not achieved')



if __name__ == '__main__':  # pragma: no cover
    # Run the doctests in this module with: $ python3 -m pottery.nextid
    import doctest
    import sys
    results = doctest.testmod()
    sys.exit(bool(results.failed))
