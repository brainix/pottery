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
'Async distributed Redis-powered monotonically increasing ID generator tests.'


import sys
import unittest.mock

from redis.asyncio import Redis as AIORedis  # type: ignore
from redis.commands.core import AsyncScript  # type: ignore
from redis.exceptions import TimeoutError

from pottery import AIONextID
from pottery import QuorumNotAchieved
from tests.base import TestCase
from tests.base import async_test


# TODO: When we drop support for Python 3.9, delete the following definition of
# aiter().
try:
    aiter  # type: ignore
except NameError:  # pragma: no cover
    aiter = iter

# TODO: When we drop support for Python 3.9, delete the following definition of
# anext().
try:
    anext  # type: ignore
except NameError:  # pragma: no cover
    # I got this anext() definition from here:
    #     https://github.com/python/cpython/blob/f4c03484da59049eb62a9bf7777b963e2267d187/Lib/test/test_asyncgen.py#L52
    _NO_DEFAULT = object()

    def anext(iterator, default=_NO_DEFAULT):
        try:
            __anext__ = type(iterator).__anext__
        except AttributeError:
            raise TypeError(f'{iterator!r} is not an async iterator')
        if default is _NO_DEFAULT:
            return __anext__(iterator)

        async def anext_impl():
            try:
                return await __anext__(iterator)
            except StopAsyncIteration:
                return default
        return anext_impl()


class AIONextIDTests(TestCase):
    'Async distributed Redis-powered monotonically increasing ID gen tests.'

    def setUp(self) -> None:
        super().setUp()
        # TODO: When we drop support for Python 3.9, delete the following if
        # condition.
        if sys.version_info > (3, 10):  # pragma: no cover
            self.aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
            self.aioids = AIONextID(masters={self.aioredis})

    async def _setup(self) -> None:
        # TODO: When we drop support for Python 3.9, delete the following if
        # condition.
        #
        # https://github.com/brainix/pottery/runs/5384161828?check_suite_focus=true
        if sys.version_info < (3, 10):  # pragma: no cover
            self.aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
            self.aioids = AIONextID(masters={self.aioredis})
        await self.aioids.reset()  # type: ignore

    @async_test
    async def test_aionextid(self):
        await self._setup()
        for expected in range(1, 10):
            with self.subTest(expected=expected):
                got = await anext(self.aioids)
                assert got == expected

    @async_test
    async def test_reset(self):
        await self._setup()
        assert await anext(self.aioids) == 1
        await self.aioids.reset()
        assert await anext(self.aioids) == 1

    @async_test
    async def test_slots(self):
        await self._setup()
        with self.assertRaises(AttributeError):
            self.aioids.__dict__

    @unittest.skipIf(sys.version_info < (3, 10), 'Python 3.10+ required')  # pragma: no cover
    @async_test
    async def test_aiter(self):
        await self._setup()
        assert aiter(self.aioids) is self.aioids

    @async_test
    async def test_anext_quorumnotachieved(self):
        await self._setup()
        with self.assertRaises(QuorumNotAchieved), \
             unittest.mock.patch.object(self.aioredis, 'get') as get:
            get.side_effect = TimeoutError
            await anext(self.aioids)

        with self.assertRaises(QuorumNotAchieved), \
             unittest.mock.patch.object(AsyncScript, '__call__') as __call__:
            __call__.side_effect = TimeoutError
            await anext(self.aioids)

    @async_test
    async def test_reset_quorumnotachieved(self):
        await self._setup()
        with self.assertRaises(QuorumNotAchieved), \
             unittest.mock.patch.object(self.aioredis, 'delete') as delete:
            delete.side_effect = TimeoutError
            await self.aioids.reset()

    @async_test
    async def test_repr(self):
        await self._setup()
        assert repr(self.aioids) == '<AIONextID key=nextid:current>'
