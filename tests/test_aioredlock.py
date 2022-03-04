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
import sys
import unittest.mock

from redis.asyncio import Redis as AIORedis  # type: ignore
from redis.commands.core import AsyncScript  # type: ignore
from redis.exceptions import TimeoutError

from pottery import AIORedlock
from pottery import ExtendUnlockedLock
from pottery import Redlock
from pottery import ReleaseUnlockedLock
from pottery.exceptions import TooManyExtensions
from tests.base import TestCase
from tests.base import async_test


class AIORedlockTests(TestCase):
    'Asynchronous distributed Redis-powered lock tests.'

    def setUp(self) -> None:
        super().setUp()
        # TODO: When we drop support for Python 3.9, delete the following if
        # condition.
        if sys.version_info > (3, 10):  # pragma: no cover
            self.aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
            self.aioredlock = AIORedlock(
                masters={self.aioredis},
                key='printer',
                auto_release_time=.2,
            )

    # TODO: When we drop support for Python 3.9, delete the following method.
    #
    # https://github.com/brainix/pottery/runs/5384161828?check_suite_focus=true
    def _setup(self) -> None:
        if sys.version_info < (3, 10):  # pragma: no cover
            self.aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
            self.aioredlock = AIORedlock(
                masters={self.aioredis},
                key='printer',
                auto_release_time=.2,
            )

    @async_test
    async def test_locked_acquire_and_release(self):
        self._setup()
        assert not await self.aioredlock.locked()
        assert await self.aioredlock.acquire()
        assert await self.aioredlock.locked()
        await self.aioredlock.release()
        assert not await self.aioredlock.locked()
        with self.assertRaises(ReleaseUnlockedLock):
            await self.aioredlock.release()

    @async_test
    async def test_extend(self):
        self._setup()
        with self.assertRaises(ExtendUnlockedLock):
            await self.aioredlock.extend()
        assert await self.aioredlock.acquire()
        for extension_num in range(Redlock._NUM_EXTENSIONS):
            with self.subTest(extension_num=extension_num):
                await self.aioredlock.extend()
        with self.assertRaises(TooManyExtensions):
            await self.aioredlock.extend()

    @async_test
    async def test_context_manager(self):
        self._setup()
        assert not await self.aioredlock.locked()
        async with self.aioredlock:
            assert await self.aioredlock.locked()
        assert not await self.aioredlock.locked()

    @async_test
    async def test_context_manager_extend(self):
        self._setup()
        with self.assertRaises(ExtendUnlockedLock):
            await self.aioredlock.extend()
        async with self.aioredlock:
            for extension_num in range(Redlock._NUM_EXTENSIONS):
                with self.subTest(extension_num=extension_num):
                    await self.aioredlock.extend()
            with self.assertRaises(TooManyExtensions):
                await self.aioredlock.extend()

    @async_test
    async def test_acquire_fails_within_auto_release_time(self):
        self._setup()
        self.aioredlock.auto_release_time = .001
        assert not await self.aioredlock.acquire(blocking=False)

    '''
    @async_test
    async def test_context_manager_fails_within_auto_release_time(self):
        self._setup()
        self.aioredlock.auto_release_time = .001
        with self.assertRaises(QuorumNotAchieved):
            async with self.aioredlock:  # pragma: no cover
                ...
    '''

    @async_test
    async def test_acquire_and_time_out(self):
        self._setup()
        assert not await self.aioredlock.locked()
        assert await self.aioredlock.acquire()
        assert await self.aioredlock.locked()
        await asyncio.sleep(self.aioredlock.auto_release_time + 1)
        assert not await self.aioredlock.locked()

    @async_test
    async def test_context_manager_time_out_before_exit(self):
        self._setup()
        with self.assertRaises(ReleaseUnlockedLock):
            async with self.aioredlock:
                await asyncio.sleep(self.aioredlock.auto_release_time + 1)
                assert not await self.aioredlock.locked()

    @async_test
    async def test_context_manager_release_before_exit(self):
        self._setup()
        with self.assertRaises(ReleaseUnlockedLock):
            async with self.aioredlock:
                await self.aioredlock.release()

    @async_test
    async def test_acquire_rediserror(self):
        self._setup()
        with unittest.mock.patch.object(self.aioredis, 'set') as set:
            set.side_effect = TimeoutError
            assert not await self.aioredlock.acquire(blocking=False)

    @async_test
    async def test_locked_rediserror(self):
        self._setup()
        async with self.aioredlock:
            assert await self.aioredlock.locked()
            with unittest.mock.patch.object(AsyncScript, '__call__') as __call__:
                __call__.side_effect = TimeoutError
                assert not await self.aioredlock.locked()

    @async_test
    async def test_extend_rediserror(self):
        self._setup()
        async with self.aioredlock:
            await self.aioredlock.extend()
            with unittest.mock.patch.object(AsyncScript, '__call__') as __call__:
                __call__.side_effect = TimeoutError
                with self.assertRaises(ExtendUnlockedLock):
                    await self.aioredlock.extend()

    @async_test
    async def test_release_rediserror(self):
        self._setup()
        with unittest.mock.patch.object(AsyncScript, '__call__') as __call__:
            __call__.side_effect = TimeoutError
            await self.aioredlock.acquire()
            with self.assertRaises(ReleaseUnlockedLock):
                await self.aioredlock.release()

    @async_test
    async def test_slots(self):
        self._setup()
        with self.assertRaises(AttributeError):
            self.aioredlock.__dict__
