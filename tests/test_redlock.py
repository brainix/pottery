# --------------------------------------------------------------------------- #
#   test_redlock.py                                                           #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #
'Distributed Redis-powered lock tests.'


import concurrent.futures
import time

from pottery import ContextTimer
from pottery import ExtendUnlockedLock
from pottery import Redlock
from pottery import ReleaseUnlockedLock
from pottery import TooManyExtensions
from pottery import synchronize
from tests.base import TestCase  # type: ignore


class RedlockTests(TestCase):
    'Distributed Redis-powered lock tests.'

    def setUp(self):
        super().setUp()
        self.redlock = Redlock(
            masters={self.redis},
            key='printer',
            auto_release_time=100,
        )

    def test_acquire_and_time_out(self):
        assert not self.redis.exists(self.redlock.key)
        assert self.redlock.acquire()
        assert self.redis.exists(self.redlock.key)
        time.sleep(self.redlock.auto_release_time / 1000 + 1)
        assert not self.redis.exists(self.redlock.key)

    def test_acquire_same_lock_twice_blocking_without_timeout(self):
        assert not self.redis.exists(self.redlock.key)
        with ContextTimer() as timer:
            assert self.redlock.acquire()
            assert self.redis.exists(self.redlock.key)
            assert self.redlock.acquire()
            assert self.redis.exists(self.redlock.key)
            assert timer.elapsed() >= self.redlock.auto_release_time

    def test_acquire_same_lock_twice_blocking_with_timeout(self):
        assert not self.redis.exists(self.redlock.key)
        assert self.redlock.acquire()
        assert self.redis.exists(self.redlock.key)
        assert not self.redlock.acquire(timeout=0)
        assert self.redis.exists(self.redlock.key)

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
                f"ReleaseUnlockedLock(masters=[Redis<ConnectionPool<Connection<host=localhost,port=6379,db={self.redis_db}>>>], "
                "key='redlock:printer')"
            )

    def test_releaseunlockedlock_str(self):
        try:
            self.redlock.release()
        except ReleaseUnlockedLock as wtf:
            assert str(wtf) == (
                f"masters=[Redis<ConnectionPool<Connection<host=localhost,port=6379,db={self.redis_db}>>>], "
                "key='redlock:printer'"
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
            "<Redlock key=redlock:printer value=b'' timeout=0>"


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

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(func) for _ in range(3)}
        results = sorted(future.result() for future in futures)
        for result1, result2 in zip(results, results[1:]):
            delta = result2 - result1
            assert 1 < delta < 2
