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



class NextId:
    'Distributed Redis-powered monotonically increasing ID generator.'

    KEY_PREFIX = 'nextid'
    KEY = 'current'
    NUM_TRIES = 3
    default_masters = frozenset({Redis()})

    def __init__(self, *, key=KEY, num_tries=NUM_TRIES, masters=default_masters):
        self.key = key
        self.num_tries = num_tries
        self.masters = masters
        self._set_id_script = self._register_set_id_script()
        self._init_masters()

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = '{}:{}'.format(self.KEY_PREFIX, value)

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
