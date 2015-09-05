#-----------------------------------------------------------------------------#
#   test_redis.py                                                             #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from redis import Redis

from pottery import RedisCounter
from pottery import RedisDeque
from pottery import RedisDict
from pottery import RedisList
from pottery import RedisSet
from tests.base import TestCase



class ContextManagerTests(TestCase):
    def setUp(self):
        super().setUp()
        self.classes = (RedisCounter, RedisDeque, RedisDict, RedisList, RedisSet)
        self.redis = Redis.from_url(self.REDIS_URL)

    def test_redis_disconnect_disconnects_from_redis(self):
        for cls in self.classes:
            num_connections = self.redis.info()['connected_clients']
            obj = cls()
            assert self.redis.info()['connected_clients'] == num_connections + 1
            obj._redis.connection_pool.disconnect()
            assert self.redis.info()['connected_clients'] == num_connections

    def test_context_manager_disconnects_from_redis(self):
        for cls in self.classes:
            num_connections = self.redis.info()['connected_clients']
            with cls() as obj:
                assert self.redis.info()['connected_clients'] == num_connections + 1
            assert self.redis.info()['connected_clients'] == num_connections

    def test_empty_containers_dont_create_redis_keys(self):
        for cls in self.classes:
            with cls() as obj:
                key = obj.key
                assert not self.redis.exists(key)
            assert not self.redis.exists(key)

    def test_counter_context_manager_deletes_temporary_redis_key(self):
        with RedisCounter('gallahad') as obj:
            key = obj.key
            assert self.redis.exists(key)
        assert not self.redis.exists(key)

        with RedisCounter({'red': 4, 'blue': 2}) as obj:
            key = obj.key
            assert self.redis.exists(key)
        assert not self.redis.exists(key)

        with RedisCounter(cats=4, dogs=8) as obj:
            key = obj.key
            assert self.redis.exists(key)
        assert not self.redis.exists(key)

    def test_dict_context_manager_deletes_temporary_redis_key(self):
        with RedisDict(jack=4098, sape=4139) as obj:
            key = obj.key
            assert self.redis.exists(key)
        assert not self.redis.exists(key)

        with RedisDict([('sape', 4139), ('guido', 4127), ('jack', 4098)]) as obj:
            key = obj.key
            assert self.redis.exists(key)
        assert not self.redis.exists(key)

    def test_list_context_manager_deletes_temporary_redis_key(self):
        with RedisList((1, 4, 9, 16, 25)) as obj:
            key = obj.key
            assert self.redis.exists(key)
        assert not self.redis.exists(key)

    def test_set_context_manager_deletes_temporary_redis_key(self):
        with RedisSet(('apple', 'orange', 'apple', 'pear', 'orange', 'banana')) as obj:
            key = obj.key
            assert self.redis.exists(key)
        assert not self.redis.exists(key)
