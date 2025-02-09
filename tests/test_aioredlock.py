# --------------------------------------------------------------------------- #
#   test_aioredlock.py                                                        #
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
'Asynchronous distributed Redis-powered lock tests.'


import asyncio
import unittest.mock

import pytest
from redis.asyncio import Redis as AIORedis
from redis.commands.core import AsyncScript
from redis.exceptions import TimeoutError

from pottery import AIORedlock
from pottery import ExtendUnlockedLock
from pottery import QuorumNotAchieved
from pottery import Redlock
from pottery import ReleaseUnlockedLock
from pottery.exceptions import TooManyExtensions


@pytest.fixture
def aioredlock(aioredis: AIORedis) -> AIORedlock:  # type: ignore
    return AIORedlock(masters={aioredis}, key='shower')


async def test_locked_acquire_and_release(aioredlock: AIORedlock) -> None:
    assert not await aioredlock.locked()
    assert await aioredlock.acquire()
    assert await aioredlock.locked()
    await aioredlock.release()
    assert not await aioredlock.locked()
    with pytest.raises(ReleaseUnlockedLock):
        await aioredlock.release()


async def test_extend(aioredlock: AIORedlock) -> None:
    with pytest.raises(ExtendUnlockedLock):
        await aioredlock.extend()
    assert await aioredlock.acquire()
    for extension_num in range(Redlock._NUM_EXTENSIONS):
        await aioredlock.extend()
    with pytest.raises(TooManyExtensions):
        await aioredlock.extend()


async def test_context_manager(aioredlock: AIORedlock) -> None:
    assert not await aioredlock.locked()
    async with aioredlock:
        assert await aioredlock.locked()
    assert not await aioredlock.locked()


async def test_context_manager_extend(aioredlock: AIORedlock) -> None:
    with pytest.raises(ExtendUnlockedLock):
        await aioredlock.extend()
    async with aioredlock:
        for extension_num in range(Redlock._NUM_EXTENSIONS):
            await aioredlock.extend()
        with pytest.raises(TooManyExtensions):
            await aioredlock.extend()


async def test_acquire_fails_within_auto_release_time(aioredlock: AIORedlock) -> None:
    aioredlock.auto_release_time = .001
    assert not await aioredlock.acquire(blocking=False)


async def test_context_manager_fails_within_auto_release_time(aioredlock: AIORedlock) -> None:
    aioredlock.auto_release_time = .001
    aioredlock.context_manager_blocking = False
    with pytest.raises(QuorumNotAchieved):
        async with aioredlock:  # pragma: no cover
            ...


async def test_acquire_and_time_out(aioredlock: AIORedlock) -> None:
    aioredlock.auto_release_time = 1
    assert not await aioredlock.locked()
    assert await aioredlock.acquire()
    assert await aioredlock.locked()
    await asyncio.sleep(aioredlock.auto_release_time)
    assert not await aioredlock.locked()


async def test_context_manager_time_out_before_exit(aioredlock: AIORedlock) -> None:
    aioredlock.auto_release_time = 1
    with pytest.raises(ReleaseUnlockedLock):
        async with aioredlock:
            await asyncio.sleep(aioredlock.auto_release_time * 2)
            assert not await aioredlock.locked()


async def test_context_manager_release_before_exit(aioredlock: AIORedlock) -> None:
    with pytest.raises(ReleaseUnlockedLock):
        async with aioredlock:
            await aioredlock.release()


def test_context_manager_nonblocking_with_timeout(aioredis: AIORedis) -> None:  # type: ignore
    with pytest.raises(ValueError):
        AIORedlock(
            masters={aioredis},
            key='shower',
            auto_release_time=.2,
            context_manager_blocking=False,
            context_manager_timeout=.1
        )


async def test_acquire_nonblocking_with_timeout(aioredlock: AIORedlock) -> None:
    with pytest.raises(ValueError):
        await aioredlock.acquire(blocking=False, timeout=.1)


async def test_acquire_rediserror(aioredlock: AIORedlock) -> None:
    aioredis = next(iter(aioredlock.masters))
    with unittest.mock.patch.object(aioredis, 'set') as set:
        set.side_effect = TimeoutError
        assert not await aioredlock.acquire(blocking=False)


async def test_locked_rediserror(aioredlock: AIORedlock) -> None:
    async with aioredlock:
        assert await aioredlock.locked()
        with unittest.mock.patch.object(AsyncScript, '__call__') as __call__:
            __call__.side_effect = TimeoutError
            assert not await aioredlock.locked()


async def test_extend_rediserror(aioredlock: AIORedlock) -> None:
    async with aioredlock:
        await aioredlock.extend()
        with unittest.mock.patch.object(AsyncScript, '__call__') as __call__:
            __call__.side_effect = TimeoutError
            with pytest.raises(ExtendUnlockedLock):
                await aioredlock.extend()


async def test_release_rediserror(aioredlock: AIORedlock) -> None:
    with unittest.mock.patch.object(AsyncScript, '__call__') as __call__:
        __call__.side_effect = TimeoutError
        await aioredlock.acquire()
        with pytest.raises(ReleaseUnlockedLock):
            await aioredlock.release()


async def test_enqueued(aioredlock: AIORedlock) -> None:
    aioredlock.auto_release_time = .2
    aioredis = next(iter(aioredlock.masters))
    aioredlock2 = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)  # type: ignore

    await aioredlock.acquire()
    # aioredlock2 is enqueued until self.aioredlock is automatically released:
    assert await aioredlock2.acquire()

    await aioredlock.acquire()
    # aioredlock2 is enqueued until the acquire timeout has expired:
    assert not await aioredlock2.acquire(timeout=0.1)


@pytest.mark.parametrize('num_locks', range(1, 11))
async def test_contention(num_locks: int) -> None:
    dbs = range(1, 6)
    urls = [f'redis://localhost:6379/{db}' for db in dbs]
    masters = [AIORedis.from_url(url, socket_timeout=1) for url in urls]
    locks = [AIORedlock(key='shower', masters=masters, auto_release_time=.2) for _ in range(num_locks)]

    try:
        coros = [lock.acquire(blocking=False) for lock in locks]
        tasks = [asyncio.create_task(coro) for coro in coros]
        done, _ = await asyncio.wait(tasks)
        results = [task.result() for task in done]
        num_unlocked = results.count(False)
        num_locked = results.count(True)
        assert num_locks-1 <= num_unlocked <= num_locks
        assert 0 <= num_locked <= 1
        # To see the following output, issue:
        # $ source venv/bin/activate; pytest -rP tests/test_aioredlock.py::test_contention; deactivate
        print(f'{num_locks} locks, {num_unlocked} unlocked, {num_locked} locked')

    finally:
        # Clean up for the next unit test run.
        coros = [lock.release() for lock in locks]  # type: ignore
        tasks = [asyncio.create_task(coro) for coro in coros]
        done, _ = await asyncio.wait(tasks)
        [task.exception() for task in done]


def test_slots(aioredlock: AIORedlock) -> None:
    with pytest.raises(AttributeError):
        aioredlock.__dict__


def test_repr(aioredlock: AIORedlock) -> None:
    assert repr(aioredlock) == '<AIORedlock key=redlock:shower>'
