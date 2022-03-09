# --------------------------------------------------------------------------- #
#   test_aioredlock.py                                                        #
#                                                                             #
#   Copyright © 2015-2022, Rajiv Bakulesh Shah, original author.              #
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

from redis.asyncio import Redis as AIORedis  # type: ignore
from redis.commands.core import AsyncScript  # type: ignore
from redis.exceptions import TimeoutError

from pottery import AIORedlock
from pottery import ExtendUnlockedLock
from pottery import QuorumNotAchieved
from pottery import Redlock
from pottery import ReleaseUnlockedLock
from pottery.exceptions import TooManyExtensions
from tests.base import TestCase
from tests.base import async_test


class AIORedlockTests(TestCase):
    'Asynchronous distributed Redis-powered lock tests.'

    @async_test
    async def test_locked_acquire_and_release(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        assert not await aioredlock.locked()
        assert await aioredlock.acquire()
        assert await aioredlock.locked()
        await aioredlock.release()
        assert not await aioredlock.locked()
        with self.assertRaises(ReleaseUnlockedLock):
            await aioredlock.release()

    @async_test
    async def test_extend(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        with self.assertRaises(ExtendUnlockedLock):
            await aioredlock.extend()
        assert await aioredlock.acquire()
        for extension_num in range(Redlock._NUM_EXTENSIONS):
            with self.subTest(extension_num=extension_num):
                await aioredlock.extend()
        with self.assertRaises(TooManyExtensions):
            await aioredlock.extend()

    @async_test
    async def test_context_manager(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        assert not await aioredlock.locked()
        async with aioredlock:
            assert await aioredlock.locked()
        assert not await aioredlock.locked()

    @async_test
    async def test_context_manager_extend(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        with self.assertRaises(ExtendUnlockedLock):
            await aioredlock.extend()
        async with aioredlock:
            for extension_num in range(Redlock._NUM_EXTENSIONS):
                with self.subTest(extension_num=extension_num):
                    await aioredlock.extend()
            with self.assertRaises(TooManyExtensions):
                await aioredlock.extend()

    @async_test
    async def test_acquire_fails_within_auto_release_time(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        aioredlock.auto_release_time = .001
        assert not await aioredlock.acquire(blocking=False)

    @async_test
    async def test_context_manager_fails_within_auto_release_time(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        aioredlock.auto_release_time = .001
        aioredlock.context_manager_blocking = False
        with self.assertRaises(QuorumNotAchieved):
            async with aioredlock:  # pragma: no cover
                ...

    @async_test
    async def test_acquire_and_time_out(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        assert not await aioredlock.locked()
        assert await aioredlock.acquire()
        assert await aioredlock.locked()
        await asyncio.sleep(aioredlock.auto_release_time + 1)
        assert not await aioredlock.locked()

    @async_test
    async def test_context_manager_time_out_before_exit(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        with self.assertRaises(ReleaseUnlockedLock):
            async with aioredlock:
                await asyncio.sleep(aioredlock.auto_release_time + 1)
                assert not await aioredlock.locked()

    @async_test
    async def test_context_manager_release_before_exit(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        with self.assertRaises(ReleaseUnlockedLock):
            async with aioredlock:
                await aioredlock.release()

    @async_test
    async def test_context_manager_nonblocking_with_timeout(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)

        with self.assertRaises(ValueError):
            AIORedlock(
                masters={aioredis},
                key='shower',
                auto_release_time=.2,
                context_manager_blocking=False,
                context_manager_timeout=.1
            )

    @async_test
    async def test_acquire_nonblocking_with_timeout(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        with self.assertRaises(ValueError):
            await aioredlock.acquire(blocking=False, timeout=.1)

    @async_test
    async def test_acquire_rediserror(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        with unittest.mock.patch.object(aioredis, 'set') as set:
            set.side_effect = TimeoutError
            assert not await aioredlock.acquire(blocking=False)

    @async_test
    async def test_locked_rediserror(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        async with aioredlock:
            assert await aioredlock.locked()
            with unittest.mock.patch.object(AsyncScript, '__call__') as __call__:
                __call__.side_effect = TimeoutError
                assert not await aioredlock.locked()

    @async_test
    async def test_extend_rediserror(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        async with aioredlock:
            await aioredlock.extend()
            with unittest.mock.patch.object(AsyncScript, '__call__') as __call__:
                __call__.side_effect = TimeoutError
                with self.assertRaises(ExtendUnlockedLock):
                    await aioredlock.extend()

    @async_test
    async def test_release_rediserror(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        with unittest.mock.patch.object(AsyncScript, '__call__') as __call__:
            __call__.side_effect = TimeoutError
            await aioredlock.acquire()
            with self.assertRaises(ReleaseUnlockedLock):
                await aioredlock.release()

    @async_test
    async def test_enqueued(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)
        aioredlock2 = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        await aioredlock.acquire()
        # aioredlock2 is enqueued until self.aioredlock is automatically released:
        assert await aioredlock2.acquire()

        await aioredlock.acquire()
        # aioredlock2 is enqueued until the acquire timeout has expired:
        assert not await aioredlock2.acquire(timeout=0.1)

    @async_test
    async def test_contention(self):
        dbs = range(1, 6)
        urls = [f'redis://localhost:6379/{db}' for db in dbs]
        masters = [AIORedis.from_url(url, socket_timeout=1) for url in urls]
        locks = [AIORedlock(key='shower', masters=masters, auto_release_time=.2) for _ in range(5)]

        try:
            coros = [lock.acquire(blocking=False) for lock in locks]
            tasks = [asyncio.create_task(coro) for coro in coros]
            done, _ = await asyncio.wait(tasks)
            results = [task.result() for task in done]
            assert 0 <= results.count(True) <= 1

        finally:
            # Clean up for the next unit test run.
            for lock in locks:
                coros = [lock.release() for lock in locks]
                tasks = [asyncio.create_task(coro) for coro in coros]
                done, _ = await asyncio.wait(tasks)
                [task.exception() for task in done]

    @async_test
    async def test_slots(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        with self.assertRaises(AttributeError):
            aioredlock.__dict__

    @async_test
    async def test_repr(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(masters={aioredis}, key='shower', auto_release_time=.2)

        assert repr(aioredlock) == '<AIORedlock key=redlock:shower>'
