#-----------------------------------------------------------------------------#
#   redlock.py                                                                #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#
'''Distributed Redis-powered lock.

Rationale and algorithm description:
    http://redis.io/topics/distlock

Reference implementations:
    https://github.com/antirez/redlock-rb
    https://github.com/SPSCommerce/redlock-py

Lua scripting:
    https://github.com/andymccurdy/redis-py#lua-scripting
'''



import concurrent.futures
import contextlib
import random
import time

from redis import Redis

from .contexttimer import contexttimer



class Redlock:
    'Distributed Redis-powered lock.'

    KEY_PREFIX = 'redlock'
    AUTO_RELEASE_TIME = 10 * 1000
    CLOCK_DRIFT_FACTOR = 0.01
    RETRY_DELAY = 200
    NUM_EXTENSIONS = 3

    default_masters = frozenset({Redis()})

    def __init__(self, *, key, masters=default_masters,
                 auto_release_time=AUTO_RELEASE_TIME, num_extensions=3):
        self.key = key
        self.masters = masters
        self.auto_release_time = auto_release_time
        self.num_extensions = num_extensions

        self._value = None
        self._extension_num = 0
        self._acquired_script = self._register_acquired_script()
        self._extend_script = self._register_extend_script()
        self._release_script = self._register_release_script()

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = '{}:{}'.format(self.KEY_PREFIX, value)

    def _register_acquired_script(self):
        master = next(iter(self.masters))
        acquired_script = master.register_script('''
            if redis.call("get", KEYS[1]) == ARGV[1] then
                local pttl = redis.call("pttl", KEYS[1])
                return (pttl > 0) and pttl or 0
            else
                return 0
            end
        ''')
        return acquired_script

    def _register_extend_script(self):
        master = next(iter(self.masters))
        extend_script = master.register_script('''
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("pexpire", KEYS[1], ARGV[2])
            else
                return 0
            end
        ''')
        return extend_script

    def _register_release_script(self):
        master = next(iter(self.masters))
        release_script = master.register_script('''
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
        ''')
        return release_script

    def _acquire_master(self, master):
        acquired = master.set(
            self.key,
            self._value,
            px=self.auto_release_time,
            nx=True,
        )
        return bool(acquired)

    def _acquired_master(self, master):
        if self._value:
            ttl = self._acquired_script(
                keys=(self.key,),
                args=(self._value,),
                client=master,
            )
        else:
            ttl = 0
        return ttl

    def _extend_master(self, master):
        extended = self._extend_script(
            keys=(self.key,),
            args=(self._value, self.auto_release_time),
            client=master,
        )
        return bool(extended)

    def _release_master(self, master):
        released = self._release_script(
            keys=(self.key,),
            args=(self._value,),
            client=master,
        )
        return bool(released)

    @property
    def _drift(self):
        return self.auto_release_time * self.CLOCK_DRIFT_FACTOR + 2

    def _acquire_masters(self):
        self._value = random.random()
        self._extension_num = 0
        num_masters_acquired = 0
        with contexttimer() as timer, \
             concurrent.futures.ThreadPoolExecutor(max_workers=len(self.masters)) as executor:
            futures = {executor.submit(self._acquire_master, master)
                       for master in self.masters}
            for future in concurrent.futures.as_completed(futures):
                num_masters_acquired += future.result()
        quorum = num_masters_acquired >= len(self.masters) // 2 + 1
        validity_time = self.auto_release_time - self._drift
        if quorum and max(validity_time, 0):
            return True
        else:
            with contextlib.suppress(RuntimeError):
                self.release()
            return False

    def acquire(self, *, blocking=True, timeout=-1):
        if blocking:
            with contexttimer() as timer:
                while timeout == -1 or timer.elapsed / 1000 < timeout:
                    if self._acquire_masters():
                        return True
                    else:
                        time.sleep(random.uniform(0, self.RETRY_DELAY/1000))
            return False
        elif timeout == -1:
            return self._acquire_masters()
        else:
            raise ValueError("can't specify a timeout for a non-blocking call")

    @property
    def acquired(self):
        with contexttimer() as timer, \
             concurrent.futures.ThreadPoolExecutor(max_workers=len(self.masters)) as executor:
            num_masters_acquired, ttls = 0, []
            futures = {executor.submit(self._acquired_master, master)
                       for master in self.masters}
            for future in concurrent.futures.as_completed(futures):
                ttl = future.result()
                num_masters_acquired += ttl > 0
                ttls.append(ttl)
            quorum = num_masters_acquired >= len(self.masters) // 2 + 1
            if quorum:
                ttls = sorted(ttls, reverse=True)
                validity_time = ttls[len(self.masters) // 2]
                validity_time -= timer.elapsed + self._drift
                return max(validity_time, 0)
            else:
                return 0

    def extend(self):
        if self._extension_num >= self.num_extensions:
            raise RuntimeError('extend lock too many times')
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.masters)) as executor:
                futures = {executor.submit(self._extend_master, master)
                           for master in self.masters}
                extended = sum(
                    future.result()
                    for future in concurrent.futures.as_completed(futures)
                )
            quorum = extended >= len(self.masters) // 2 + 1
            self._extension_num += quorum
            return quorum

    def release(self):
        num_masters_released = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.masters)) as executor:
            futures = {executor.submit(self._release_master, master)
                       for master in self.masters}
            for future in concurrent.futures.as_completed(futures):
                num_masters_released += future.result()
        quorum = num_masters_released >= len(self.masters) // 2 + 1
        if not quorum:
            raise RuntimeError('release unlocked lock')

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
