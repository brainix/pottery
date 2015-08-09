#-----------------------------------------------------------------------------#
#   test_redis.py                                                             #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from redis import Redis

from pottery import monkey
from tests.base import TestCase



class RedisTests(TestCase):
    def test_redis_clients_equal_if_same_url(self):
        # The Redis client doesn't have a sane equality test.  Monkey patch the
        # Redis client so that two client instances are equal if they're
        # connected to the same Redis host, port, and database.
        redis1 = Redis.from_url(self.REDIS_URL)
        redis2 = Redis.from_url(self.REDIS_URL)
        assert redis1 == redis2
