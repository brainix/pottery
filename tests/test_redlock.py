# --------------------------------------------------------------------------- #
#   test_redlock.py                                                           #
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
'Distributed Redis-powered lock tests.'


import concurrent.futures
import time
import unittest.mock

from redis.exceptions import TimeoutError

from pottery import ContextTimer
from pottery import ExtendUnlockedLock
from pottery import QuorumIsImpossible
from pottery import Redlock
from pottery import ReleaseUnlockedLock
from pottery import TooManyExtensions
from pottery import synchronize
from pottery.redlock import _logger
from tests.base import TestCase  # type: ignore


class RedlockTests(TestCase):
    'Distributed Redis-powered lock tests.'

    def setUp(self):
        super().setUp()
        self.redlock = Redlock(
            masters={self.redis},
            key='printer',
            auto_release_time=200,
        )

    def test_acquire_and_time_out(self):
        assert not self.redis.exists(self.redlock.key)
        assert self.redlock.acquire()
        assert self.redis.exists(self.redlock.key)
        time.sleep(self.redlock.auto_release_time / 1000 + 1)
        assert not self.redis.exists(self.redlock.key)

    def test_acquire_same_lock_twice_blocking_without_timeout(self):
        assert not self.redis.exists(self.redlock.key)
        with ContextTimer() as timer, \
             unittest.mock.patch.object(_logger, 'info') as info:
            assert self.redlock.acquire()
            assert self.redis.exists(self.redlock.key)
            assert self.redlock.acquire()
            assert self.redis.exists(self.redlock.key)
            assert timer.elapsed() >= self.redlock.auto_release_time
            assert info.call_count == 1, f'_logger.info() called {info.call_count} times'

    def test_acquire_same_lock_twice_blocking_with_timeout(self):
        with unittest.mock.patch.object(_logger, 'info') as info:
            assert not self.redis.exists(self.redlock.key)
            assert self.redlock.acquire()
            assert self.redis.exists(self.redlock.key)
            assert not self.redlock.acquire(timeout=0)
            assert not self.redlock.acquire(timeout=0.025)
            assert self.redis.exists(self.redlock.key)
            assert info.call_count == 1, f'_logger.info() called {info.call_count} times'

    def test_acquire_same_lock_twice_non_blocking_without_timeout(self):
        assert not self.redis.exists(self.redlock.key)
        assert self.redlock.acquire()
        assert self.redis.exists(self.redlock.key)
        assert not self.redlock.acquire(blocking=False)
        assert self.redis.exists(self.redlock.key)
        time.sleep(self.redlock.auto_release_time / 1000 + 1)
        assert not self.redis.exists(self.redlock.key)

    def test_acquire_same_lock_twice_non_blocking_with_timeout(self):
        assert not self.redis.exists(self.redlock.key)
        assert self.redlock.acquire()
        assert self.redis.exists(self.redlock.key)
        with self.assertRaises(ValueError):
            self.redlock.acquire(blocking=False, timeout=0)
        assert self.redis.exists(self.redlock.key)

    def test_acquired(self):
        assert not self.redis.exists(self.redlock.key)
        assert not self.redlock.locked()
        assert self.redlock.acquire()
        assert self.redis.exists(self.redlock.key)
        assert self.redlock.locked()
        time.sleep(self.redlock.auto_release_time / 1000 + 1)
        assert not self.redis.exists(self.redlock.key)
        assert not self.redlock.locked()

    def test_extend(self):
        assert not self.redis.exists(self.redlock.key)
        with self.assertRaises(ExtendUnlockedLock):
            self.redlock.extend()
        assert self.redlock.acquire()
        for extension_num in range(3):
            with self.subTest(extension_num=extension_num):
                self.redlock.extend()
        with self.assertRaises(TooManyExtensions):
            self.redlock.extend()

    def test_acquire_then_release(self):
        assert not self.redis.exists(self.redlock.key)
        assert self.redlock.acquire()
        assert self.redis.exists(self.redlock.key)
        self.redlock.release()
        assert not self.redis.exists(self.redlock.key)

    def test_release_unlocked_lock(self):
        with self.assertRaises(ReleaseUnlockedLock):
            self.redlock.release()

    def test_releaseunlockedlock_repr(self):
        try:
            self.redlock.release()
        except ReleaseUnlockedLock as wtf:
            assert repr(wtf) == (
                "ReleaseUnlockedLock(key='redlock:printer', "
                f"masters=[Redis<ConnectionPool<Connection<host=localhost,port=6379,db={self.redis_db}>>>], "
                "redis_errors=[])"
            )

    def test_releaseunlockedlock_str(self):
        try:
            self.redlock.release()
        except ReleaseUnlockedLock as wtf:
            assert str(wtf) == (
                "key='redlock:printer', "
                f"masters=[Redis<ConnectionPool<Connection<host=localhost,port=6379,db={self.redis_db}>>>], "
                "redis_errors=[]"
            )

    def test_release_same_lock_twice(self):
        assert not self.redis.exists(self.redlock.key)
        assert self.redlock.acquire()
        self.redlock.release()
        with self.assertRaises(ReleaseUnlockedLock):
            self.redlock.release()

    def test_context_manager(self):
        assert not self.redis.exists(self.redlock.key)
        with self.redlock:
            assert self.redis.exists(self.redlock.key)
        assert not self.redis.exists(self.redlock.key)

    def test_context_manager_time_out_before_exit(self):
        assert not self.redis.exists(self.redlock.key)
        with self.assertRaises(ReleaseUnlockedLock), self.redlock:
            assert self.redis.exists(self.redlock.key)
            time.sleep(self.redlock.auto_release_time / 1000 + 1)
            assert not self.redis.exists(self.redlock.key)
        assert not self.redis.exists(self.redlock.key)

    def test_context_manager_acquired(self):
        assert not self.redis.exists(self.redlock.key)
        assert not self.redlock.locked()
        with self.redlock:
            assert self.redis.exists(self.redlock.key)
            assert self.redlock.locked()
        assert not self.redis.exists(self.redlock.key)
        assert not self.redlock.locked()

    def test_context_manager_acquired_time_out_before_exit(self):
        assert not self.redis.exists(self.redlock.key)
        assert not self.redlock.locked()
        with self.assertRaises(ReleaseUnlockedLock), self.redlock:
            assert self.redis.exists(self.redlock.key)
            assert self.redlock.locked()
            time.sleep(self.redlock.auto_release_time / 1000 + 1)
            assert not self.redis.exists(self.redlock.key)
            assert not self.redlock.locked()
        assert not self.redis.exists(self.redlock.key)
        assert not self.redlock.locked()

    def test_context_manager_release_before_exit(self):
        assert not self.redis.exists(self.redlock.key)
        with self.assertRaises(ReleaseUnlockedLock), self.redlock:
            assert self.redis.exists(self.redlock.key)
            self.redlock.release()
            assert not self.redis.exists(self.redlock.key)

    def test_repr(self):
        assert repr(self.redlock) == \
            "<Redlock key=redlock:printer UUID= timeout=0>"

    def test_acquire_rediserror(self):
        with unittest.mock.patch.object(self.redis, 'set') as set:
            set.side_effect = TimeoutError
            assert not self.redlock.acquire(blocking=False)

    def test_acquire_no_validity_time(self):
        self.redlock.CLOCK_DRIFT_FACTOR = 1
        assert not self.redlock.acquire(blocking=False)

    def test_acquire_quorumisimpossible(self):
        with unittest.mock.patch.object(self.redis, 'set') as set, \
             self.assertRaises(QuorumIsImpossible):
            set.side_effect = TimeoutError
            self.redlock.acquire(raise_on_redis_errors=True)

    def test_locked_rediserror(self):
        with self.redlock, \
             unittest.mock.patch.object(self.redlock, '_acquired_script') as _acquired_script:
            _acquired_script.side_effect = TimeoutError
            assert not self.redlock.locked()

    def test_locked_quorumisimpossible(self):
        with self.redlock, \
             unittest.mock.patch.object(self.redlock, '_acquired_script') as _acquired_script, \
             self.assertRaises(QuorumIsImpossible):
            _acquired_script.side_effect = TimeoutError
            self.redlock.locked(raise_on_redis_errors=True)

    def test_extend_rediserror(self):
        with self.redlock, \
             unittest.mock.patch.object(self.redlock, '_extend_script') as _extend_script, \
             self.assertRaises(ExtendUnlockedLock):
            _extend_script.side_effect = TimeoutError
            self.redlock.extend()

    def test_extend_quorumisimpossible(self):
        with self.redlock, \
             unittest.mock.patch.object(self.redlock, '_extend_script') as _extend_script, \
             self.assertRaises(QuorumIsImpossible):
            _extend_script.side_effect = TimeoutError
            self.redlock.extend(raise_on_redis_errors=True)

    def test_release_rediserror(self):
        with self.redlock, \
             unittest.mock.patch.object(self.redlock, '_release_script') as _release_script, \
             self.assertRaises(ReleaseUnlockedLock):
            _release_script.side_effect = TimeoutError
            self.redlock.release()

    def test_release_quorumisimpossible(self):
        with self.redlock, \
             unittest.mock.patch.object(self.redlock, '_release_script') as _release_script, \
             self.assertRaises(QuorumIsImpossible):
            _release_script.side_effect = TimeoutError
            self.redlock.release(raise_on_redis_errors=True)


class SynchronizeTests(TestCase):
    def test_synchronize(self):
        @synchronize(
            key='synchronized-func',
            masters={self.redis},
            auto_release_time=1500,
        )
        def func():
            time.sleep(1)
            return time.time()

        with unittest.mock.patch.object(_logger, 'info') as info:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(func) for _ in range(3)}
            results = sorted(future.result() for future in futures)
            for result1, result2 in zip(results, results[1:]):
                delta = result2 - result1
                assert 1 < delta < 2
            assert info.call_count == 5, f'_logger.info() called {info.call_count} times'
