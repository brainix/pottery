# --------------------------------------------------------------------------- #
#   test_redis.py                                                             #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


from redis import Redis

from pottery import monkey
from tests.base import TestCase


class RedisTests(TestCase):
    _REDIS_URL = 'redis://localhost:6379/'

    def test_redis_clients_equal_if_same_url(self):
        # The Redis client doesn't have a sane equality test.  So we've monkey
        # patched the Redis client so that two client instances are equal if
        # they're connected to the same Redis host, port, and database.
        redis1 = Redis.from_url(self._REDIS_URL)
        redis2 = Redis.from_url(self._REDIS_URL)
        assert redis1 == redis2
        assert not redis1 != redis2
        assert redis1 != None
        assert not redis1 == None
