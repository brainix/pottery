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
import unittest

from redis.asyncio import Redis as AIORedis  # type: ignore

from pottery import AIONextID
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
        self.redis.unlink('nextid:current')
        # TODO: When we drop support for Python 3.9, delete the following if
        # condition.
        if sys.version_info > (3, 10):  # pragma: no cover
            self.aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
            self.aioids = AIONextID(masters={self.aioredis})

    # TODO: When we drop support for Python 3.9, delete the following method.
    #
    # https://github.com/brainix/pottery/runs/5384161828?check_suite_focus=true
    def _setup(self) -> None:
        if sys.version_info < (3, 10):  # pragma: no cover
            self.aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
            self.aioids = AIONextID(masters={self.aioredis})

    @async_test
    async def test_aionextid(self):
        self._setup()
        for expected in range(1, 10):
            with self.subTest(expected=expected):
                got = await anext(self.aioids)
                assert got == expected

    @unittest.skipIf(sys.version_info < (3, 10), 'Python 3.10+ required')  # pragma: no cover
    def test_aiter(self):
        self._setup()
        assert aiter(self.aioids) is self.aioids

    @async_test
    async def test_slots(self):
        self._setup()
        with self.assertRaises(AttributeError):
            self.aioids.__dict__
