#-----------------------------------------------------------------------------#
#   test_context_manager.py                                                   #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
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
        self.classes = {RedisCounter, RedisDeque, RedisDict, RedisList, RedisSet}
        self.redis = Redis.from_url(self.REDIS_URL)

    def test_redis_disconnect_disconnects_from_redis(self):
        num_connections = self.redis.info()['connected_clients']
        for cls in self.classes:
            with self.subTest(cls=cls):
                obj = cls()
                assert self.redis.info()['connected_clients'] == num_connections + 1
                obj._redis.connection_pool.disconnect()
                assert self.redis.info()['connected_clients'] == num_connections

    def test_context_manager_disconnects_from_redis(self):
        num_connections = self.redis.info()['connected_clients']
        for cls in self.classes:
            with self.subTest(cls=cls):
                with cls() as obj:
                    assert self.redis.info()['connected_clients'] == num_connections + 1
                assert self.redis.info()['connected_clients'] == num_connections

    def test_empty_containers_dont_create_redis_keys(self):
        for cls in self.classes:
            with self.subTest(cls=cls):
                with cls() as obj:
                    assert not self.redis.exists(obj.key)
                assert not self.redis.exists(obj.key)

    def test_context_manager_deletes_temporary_redis_key(self):
        class_args_kwargs = (
            (RedisCounter, ('gallahad',), {}),
            (RedisCounter, ({'red': 4, 'blue': 2},), {}),
            (RedisCounter, tuple(), {'cats': 4, 'dogs': 8}),
            (RedisDict, tuple(), {'jack': 4098, 'sape': 4139}),
            (RedisDict, ([('sape', 4139), ('guido', 4127), ('jack', 4098)],), {}),
            (RedisList, ((1, 4, 9, 16, 25),), {}),
            (RedisSet, (('apple', 'orange', 'apple', 'pear', 'orange', 'banana'),), {}),
        )
        for cls, args, kwargs in class_args_kwargs:
            with self.subTest(cls=cls, args=args, kwargs=kwargs):
                with cls(*args, **kwargs) as obj:
                    assert self.redis.exists(obj.key)
                assert not self.redis.exists(obj.key)
