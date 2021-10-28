# --------------------------------------------------------------------------- #
#   test_redis.py                                                             #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
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


from redis import Redis

from pottery import monkey
from tests.base import TestCase


class RedisTests(TestCase):
    _REDIS_URL = 'redis://localhost:6379/'

    def test_redis_connection_pools_equal_if_same_url(self):
        # The Redis client connection pool doesn't have a sane equality test.
        # So we've monkey patched the connection pool so that two connection
        # pool instances are equal if they're connected to the same Redis host,
        # port, and database.
        redis1 = Redis.from_url(self._REDIS_URL)
        redis2 = Redis.from_url(self._REDIS_URL)
        assert redis1.connection_pool == redis2.connection_pool
        assert not redis1.connection_pool != redis2.connection_pool
        assert redis1.connection_pool != None
        assert not redis1.connection_pool == None
