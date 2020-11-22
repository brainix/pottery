# --------------------------------------------------------------------------- #
#   test_redlock.py                                                           #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #
'Distributed Redis-powered lock tests.'


import contextlib
import gc
import time

from pottery import ContextTimer
from pottery import ExtendUnlockedLock
from pottery import Redlock
from pottery import ReleaseUnlockedLock
from pottery import TooManyExtensions
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

    def tearDown(self):
        with contextlib.suppress(AttributeError, ReleaseUnlockedLock):
            self.redlock.release()
        super().tearDown()

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
                "ReleaseUnlockedLock(masters=[Redis<ConnectionPool<Connection<host=localhost,port=6379,db=0>>>], "
                "key='redlock:printer')"
            )

    def test_releaseunlockedlock_str(self):
        try:
            self.redlock.release()
        except ReleaseUnlockedLock as wtf:
            assert str(wtf) == (
                "masters=[Redis<ConnectionPool<Connection<host=localhost,port=6379,db=0>>>], "
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

    def test_unlocks_on_del(self):
        key = self.redlock.key
        assert self.redlock.acquire()
        assert self.redis.exists(key)
        del self.redlock
        gc.collect()
        assert not self.redis.exists(key)

    def test_repr(self):
        assert repr(self.redlock) == \
            "<Redlock key=redlock:printer value=b'' timeout=0>"
