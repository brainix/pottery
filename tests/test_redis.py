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

    def test_client_equality(self):
        r1 = Redis.from_url('redis://localhost:6379/9')
        r2 = Redis.from_url('redis://localhost:6379/9')
        assert r1 == r2
        assert hash(r1) == hash(r2)

    def test_clients_unequal_if_different_types(self):
        r = Redis.from_url('redis://localhost:6379/9')
        assert r != 0

    def test_clients_unequal_if_different_hosts(self):
        r1 = Redis.from_url('redis://localhost:6379/9')
        r2 = Redis.from_url('redis://127.0.0.1:6379/9')
        assert r1 != r2
        assert hash(r1) != hash(r2)

    def test_clients_unequal_if_different_ports(self):
        r1 = Redis.from_url('redis://localhost:6379/9')
        r2 = Redis.from_url('redis://localhost:6380/9')
        assert r1 != r2
        assert hash(r1) != hash(r2)

    def test_clients_unequal_if_different_dbs(self):
        r1 = Redis.from_url('redis://localhost:6379/9')
        r2 = Redis.from_url('redis://localhost:6380/10')
        assert r1 != r2
        assert hash(r1) != hash(r2)
