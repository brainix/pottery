# --------------------------------------------------------------------------- #
#   test_redlock.py                                                           #
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
'Distributed Redis-powered lock tests.'


import concurrent.futures
import contextlib
import time
import unittest.mock

import pytest
from redis import Redis
from redis.commands.core import Script
from redis.exceptions import TimeoutError

from pottery import ContextTimer
from pottery import ExtendUnlockedLock
from pottery import QuorumIsImpossible
from pottery import QuorumNotAchieved
from pottery import Redlock
from pottery import ReleaseUnlockedLock
from pottery import TooManyExtensions
from pottery import synchronize
from pottery.base import logger


class TestRedlock:
    'Distributed Redis-powered lock tests.'

    @staticmethod
    @pytest.fixture
    def redlock(redis: Redis) -> Redlock:
        return Redlock(masters={redis}, key='printer', auto_release_time=.2)

    @staticmethod
    def test_acquire_fails_within_auto_release_time(redlock: Redlock) -> None:
        redlock.auto_release_time = .001
        assert not redlock._acquire_masters()

    @staticmethod
    def test_acquire_and_time_out(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        assert redlock.acquire()
        assert redis.exists(redlock.key)
        time.sleep(redlock.auto_release_time * 2)
        assert not redis.exists(redlock.key)

    @staticmethod
    def test_acquire_same_lock_twice_blocking_without_timeout(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        with ContextTimer() as timer, \
             unittest.mock.patch.object(logger, 'info') as info:
            assert redlock.acquire()
            assert redis.exists(redlock.key)
            assert redlock.acquire()
            assert redis.exists(redlock.key)
            assert timer.elapsed() / 1000 >= redlock.auto_release_time
            assert info.call_count == 1, f'_logger.info() called {info.call_count} times'

    @staticmethod
    def test_acquire_same_lock_twice_non_blocking_without_timeout(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        assert redlock.acquire()
        assert redis.exists(redlock.key)
        assert not redlock.acquire(blocking=False)
        assert redis.exists(redlock.key)
        time.sleep(redlock.auto_release_time * 2)
        assert not redis.exists(redlock.key)

    @staticmethod
    def test_acquire_same_lock_twice_non_blocking_with_timeout(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        assert redlock.acquire()
        assert redis.exists(redlock.key)
        with pytest.raises(ValueError):
            redlock.acquire(blocking=False, timeout=0)
        assert redis.exists(redlock.key)

    @staticmethod
    def test_acquired(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        assert not redlock.locked()
        assert redlock.acquire()
        assert redis.exists(redlock.key)
        assert redlock.locked()
        time.sleep(redlock.auto_release_time * 2)
        assert not redis.exists(redlock.key)
        assert not redlock.locked()

    @staticmethod
    def test_extend(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        with pytest.raises(ExtendUnlockedLock):
            redlock.extend()
        assert redlock.acquire()
        for extension_num in range(Redlock._NUM_EXTENSIONS):
            redlock.extend()
        with pytest.raises(TooManyExtensions):
            redlock.extend()

    @staticmethod
    def test_acquire_then_release(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        assert redlock.acquire()
        assert redis.exists(redlock.key)
        redlock.release()
        assert not redis.exists(redlock.key)

    @staticmethod
    def test_release_unlocked_lock(redlock: Redlock) -> None:
        with pytest.raises(ReleaseUnlockedLock):
            redlock.release()

    @staticmethod
    def test_releaseunlockedlock_repr(redlock: Redlock) -> None:
        try:
            redlock.release()
        except ReleaseUnlockedLock as wtf:
            redis = next(iter(redlock.masters))
            redis_db = redis.get_connection_kwargs()['db']  # type: ignore
            assert repr(wtf) == (
                "ReleaseUnlockedLock(key='redlock:printer', "
                f"masters=frozenset({{<redis.client.Redis(<redis.connection.ConnectionPool(<redis.connection.Connection(host=localhost,port=6379,db={redis_db})>)>)>}}), "
                "redis_errors=[])"
            )

    @staticmethod
    def test_release_same_lock_twice(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        assert redlock.acquire()
        redlock.release()
        with pytest.raises(ReleaseUnlockedLock):
            redlock.release()

    @staticmethod
    def test_context_manager(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        with redlock:
            assert redis.exists(redlock.key)
        assert not redis.exists(redlock.key)

    @staticmethod
    def test_context_manager_time_out_before_exit(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        with pytest.raises(ReleaseUnlockedLock), redlock:
            assert redis.exists(redlock.key)
            time.sleep(redlock.auto_release_time * 2)
            assert not redis.exists(redlock.key)
        assert not redis.exists(redlock.key)

    @staticmethod
    def test_context_manager_acquired(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        assert not redlock.locked()
        with redlock:
            assert redis.exists(redlock.key)
            assert redlock.locked()
        assert not redis.exists(redlock.key)
        assert not redlock.locked()

    @staticmethod
    def test_context_manager_acquired_time_out_before_exit(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        assert not redlock.locked()
        with pytest.raises(ReleaseUnlockedLock), redlock:
            assert redis.exists(redlock.key)
            assert redlock.locked()
            time.sleep(redlock.auto_release_time * 2)
            assert not redis.exists(redlock.key)
            assert not redlock.locked()
        assert not redis.exists(redlock.key)
        assert not redlock.locked()

    @staticmethod
    def test_context_manager_release_before_exit(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        assert not redis.exists(redlock.key)
        with pytest.raises(ReleaseUnlockedLock), redlock:
            assert redis.exists(redlock.key)
            redlock.release()
            assert not redis.exists(redlock.key)

    @staticmethod
    def test_invalid_context_manager_params(redis: Redis) -> None:
        with pytest.raises(ValueError):
            Redlock(
                masters={redis},
                key='printer',
                context_manager_blocking=False,
                context_manager_timeout=0.2,
            )

    @staticmethod
    def test_default_context_manager_params(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        redlock2 = Redlock(masters={redis}, key='printer', auto_release_time=.2)
        with contextlib.suppress(ReleaseUnlockedLock), redlock:
            assert redlock.locked()
            assert not redlock2.locked()
            with redlock2:
                assert not redlock.locked()
                assert redlock2.locked()

    @staticmethod
    def test_overridden_context_manager_params(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        redlock2 = Redlock(
            masters={redis},
            key='printer',
            auto_release_time=.2,
            context_manager_blocking=False,
        )
        with redlock, pytest.raises(QuorumNotAchieved):
            with redlock2:
                ...  # pragma: no cover

    @staticmethod
    def test_repr(redlock: Redlock) -> None:
        assert repr(redlock) == '<Redlock key=redlock:printer>'

    @staticmethod
    def test_slots(redlock: Redlock) -> None:
        with pytest.raises(AttributeError):
            redlock.__dict__

    @staticmethod
    def test_acquire_rediserror(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        with unittest.mock.patch.object(redis, 'set') as set:
            set.side_effect = TimeoutError
            assert not redlock.acquire(blocking=False)

    @staticmethod
    def test_acquire_quorumisimpossible(redlock: Redlock) -> None:
        redis = next(iter(redlock.masters))
        with unittest.mock.patch.object(redis, 'set') as set, \
             pytest.raises(QuorumIsImpossible):
            set.side_effect = TimeoutError
            redlock.acquire(raise_on_redis_errors=True)

    @staticmethod
    def test_locked_rediserror(redlock: Redlock) -> None:
        with redlock, \
             unittest.mock.patch.object(Script, '__call__') as __call__:
            __call__.side_effect = TimeoutError
            assert not redlock.locked()

    @staticmethod
    def test_locked_quorumisimpossible(redlock: Redlock) -> None:
        with redlock, \
             unittest.mock.patch.object(Script, '__call__') as __call__, \
             pytest.raises(QuorumIsImpossible):
            __call__.side_effect = TimeoutError
            redlock.locked(raise_on_redis_errors=True)

    @staticmethod
    def test_extend_rediserror(redlock: Redlock) -> None:
        with redlock, \
             unittest.mock.patch.object(Script, '__call__') as __call__, \
             pytest.raises(ExtendUnlockedLock):
            __call__.side_effect = TimeoutError
            redlock.extend()

    @staticmethod
    def test_extend_quorumisimpossible(redlock: Redlock) -> None:
        with redlock, \
             unittest.mock.patch.object(Script, '__call__') as __call__, \
             pytest.raises(QuorumIsImpossible):
            __call__.side_effect = TimeoutError
            redlock.extend(raise_on_redis_errors=True)

    @staticmethod
    def test_release_rediserror(redlock: Redlock) -> None:
        with redlock, \
             unittest.mock.patch.object(Script, '__call__') as __call__, \
             pytest.raises(ReleaseUnlockedLock):
            __call__.side_effect = TimeoutError
            redlock.release()

    @staticmethod
    def test_release_quorumisimpossible(redlock: Redlock) -> None:
        with redlock, \
             unittest.mock.patch.object(Script, '__call__') as __call__, \
             pytest.raises(QuorumIsImpossible):
            __call__.side_effect = TimeoutError
            redlock.release(raise_on_redis_errors=True)

    @staticmethod
    @pytest.mark.parametrize('num_locks', range(1, 11))
    def test_contention(num_locks: int) -> None:
        dbs = range(1, 6)
        urls = [f'redis://localhost:6379/{db}' for db in dbs]
        masters = [Redis.from_url(url, socket_timeout=1) for url in urls]
        locks = [Redlock(key='printer', masters=masters, auto_release_time=.2) for _ in range(num_locks)]

        try:
            num_unlocked, num_locked = 0, 0
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [executor.submit(lock.acquire, blocking=False) for lock in locks]
                for future in concurrent.futures.as_completed(futures):
                    locked = future.result()
                    num_unlocked += not locked
                    num_locked += locked
            assert num_locks-1 <= num_unlocked <= num_locks
            assert 0 <= num_locked <= 1
            # To see the following output, issue:
            # $ source venv/bin/activate; pytest -rP tests/test_redlock.py::TestRedlock::test_contention; deactivate
            print(f'{num_locks} locks, {num_unlocked} unlocked, {num_locked} locked')

        finally:
            # Clean up for the next unit test run.
            with contextlib.suppress(ReleaseUnlockedLock):
                for lock in locks:  # pragma: no cover
                    lock.release()


class TestSynchronize:
    @staticmethod
    def test_synchronize(redis: Redis) -> None:
        @synchronize(key='synchronized-func', masters={redis}, auto_release_time=.2)
        def func() -> float:
            time.sleep(.1)
            return time.time()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(func) for _ in range(3)}
        results = sorted(future.result() for future in futures)
        for result1, result2 in zip(results, results[1:]):
            delta = result2 - result1
            assert delta > .1

    @staticmethod
    def test_synchronize_fails(redis: Redis) -> None:
        @synchronize(key='synchronized-func', masters={redis}, auto_release_time=.001, blocking=False)
        def func() -> None:
            raise NotImplementedError

        with pytest.raises(QuorumNotAchieved):
            func()
