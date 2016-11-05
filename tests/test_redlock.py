#-----------------------------------------------------------------------------#
#   test_redlock.py                                                           #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#
'Distributed Redis-powered lock tests.'



import contextlib
import time

from redis import Redis

from pottery import contexttimer
from pottery import Redlock
from tests.base import TestCase



class RedlockTests(TestCase):
    'Distributed Redis-powered lock tests.'

    def setUp(self):
        super().setUp()
        self.redis = Redis()
        self.redlock = Redlock(
            masters={self.redis},
            key='management-command',
            auto_release_time=100,
        )

    def tearDown(self):
        with contextlib.suppress(RuntimeError):
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
        with contexttimer() as timer:
            assert self.redlock.acquire()
            assert self.redis.exists(self.redlock.key)
            assert self.redlock.acquire()
            assert self.redis.exists(self.redlock.key)
            assert timer.elapsed >= self.redlock.auto_release_time

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
        assert not self.redlock.acquired
        assert self.redlock.acquire()
        assert self.redis.exists(self.redlock.key)
        assert self.redlock.acquired
        time.sleep(self.redlock.auto_release_time / 1000 + 1)
        assert not self.redis.exists(self.redlock.key)
        assert not self.redlock.acquired

    def test_extend(self):
        assert not self.redis.exists(self.redlock.key)
        assert not self.redlock.extend()
        assert self.redlock.acquire()
        for extension_num in range(3):
            with self.subTest(extension_num=extension_num):
                assert self.redlock.extend()
        with self.assertRaises(RuntimeError):
            self.redlock.extend()

    def test_acquire_then_release(self):
        assert not self.redis.exists(self.redlock.key)
        assert self.redlock.acquire()
        assert self.redis.exists(self.redlock.key)
        self.redlock.release()
        assert not self.redis.exists(self.redlock.key)

    def test_release_unlocked_lock(self):
        with self.assertRaises(RuntimeError):
            self.redlock.release()

    def test_release_same_lock_twice(self):
        assert not self.redis.exists(self.redlock.key)
        assert self.redlock.acquire()
        self.redlock.release()
        with self.assertRaises(RuntimeError):
            self.redlock.release()

    def test_context_manager(self):
        assert not self.redis.exists(self.redlock.key)
        with self.redlock:
            assert self.redis.exists(self.redlock.key)
        assert not self.redis.exists(self.redlock.key)

    def test_context_manager_time_out_before_exit(self):
        assert not self.redis.exists(self.redlock.key)
        with self.assertRaises(RuntimeError):
            with self.redlock:
                assert self.redis.exists(self.redlock.key)
                time.sleep(self.redlock.auto_release_time / 1000 + 1)
                assert not self.redis.exists(self.redlock.key)
        assert not self.redis.exists(self.redlock.key)

    def test_context_manager_acquired(self):
        assert not self.redis.exists(self.redlock.key)
        assert not self.redlock.acquired
        with self.redlock:
            assert self.redis.exists(self.redlock.key)
            assert self.redlock.acquired
        assert not self.redis.exists(self.redlock.key)
        assert not self.redlock.acquired

    def test_context_manager_acquired_time_out_before_exit(self):
        assert not self.redis.exists(self.redlock.key)
        assert not self.redlock.acquired
        with self.assertRaises(RuntimeError):
            with self.redlock:
                assert self.redis.exists(self.redlock.key)
                assert self.redlock.acquired
                time.sleep(self.redlock.auto_release_time / 1000 + 1)
                assert not self.redis.exists(self.redlock.key)
                assert not self.redlock.acquired
        assert not self.redis.exists(self.redlock.key)
        assert not self.redlock.acquired

    def test_context_manager_release_before_exit(self):
        assert not self.redis.exists(self.redlock.key)
        with self.assertRaises(RuntimeError):
            with self.redlock:
                assert self.redis.exists(self.redlock.key)
                self.redlock.release()
                assert not self.redis.exists(self.redlock.key)
