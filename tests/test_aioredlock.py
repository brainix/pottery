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


import contextlib

from pottery import AIORedlock
from pottery import ReleaseUnlockedLock
from tests.base import TestCase
from tests.base import async_test


class AIORedlockTests(TestCase):
    'Asynchronous distributed Redis-powered lock tests.'

    def setUp(self) -> None:
        super().setUp()
        self.aioredlock = AIORedlock(
            masters={self.aioredis},
            key='printer',
            auto_release_time=.2,
        )

    def test_slots(self):
        with self.assertRaises(AttributeError):
            self.aioredlock.__dict__

    @async_test
    async def test_acquire(self):
        try:
            assert await self.aioredlock.acquire()
        finally:
            with contextlib.suppress(ReleaseUnlockedLock):
                await self.aioredlock.release()
