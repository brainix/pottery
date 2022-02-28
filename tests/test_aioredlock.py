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


from redis.asyncio import Redis as AIORedis  # type: ignore

from pottery import AIORedlock
from pottery import ReleaseUnlockedLock
from tests.base import TestCase
from tests.base import async_test


class AIORedlockTests(TestCase):
    'Asynchronous distributed Redis-powered lock tests.'

    @async_test
    async def test_slots(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(
            masters={aioredis},
            key='printer',
            auto_release_time=.2,
        )
        with self.assertRaises(AttributeError):
            aioredlock.__dict__

    @async_test
    async def test_locked_acquire_and_release(self):
        aioredis = AIORedis.from_url(self.redis_url, socket_timeout=1)
        aioredlock = AIORedlock(
            masters={aioredis},
            key='printer',
            auto_release_time=.2,
        )
        assert not await aioredlock.locked()
        assert await aioredlock.acquire()
        assert await aioredlock.locked()
        await aioredlock.release()
        assert not await aioredlock.locked()
        with self.assertRaises(ReleaseUnlockedLock):
            await aioredlock.release()
