#-----------------------------------------------------------------------------#
#   test_cache.py                                                             #
#                                                                             #
#   Copyright © 2015-2018, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import random
import time

from pottery import RedisDict
from pottery import redis_cache
from pottery.base import _default_redis
from pottery.cache import _DEFAULT_TIMEOUT
from tests.base import TestCase



class CacheTests(TestCase):
    _KEY = 'expensive-method'

    def setUp(self):
        super().setUp()
        self.redis = _default_redis
        self.cache = RedisDict(key=self._KEY)
        self.redis.delete(self._KEY)

    def tearDown(self):
        self.redis.delete(self._KEY)
        super().tearDown()

    @staticmethod
    @redis_cache(key=_KEY)
    def expensive_method(*args, **kwargs):
        return random.random()

    def test_cache(self):
        assert len(self.cache) == 0

        value1 = self.expensive_method()
        assert len(self.cache) == 1
        assert self.expensive_method() == value1
        assert len(self.cache) == 1

        value2 = self.expensive_method('raj')
        assert len(self.cache) == 2
        assert value2 != value1
        assert self.expensive_method('raj') == value2
        assert len(self.cache) == 2

        value3 = self.expensive_method(first='raj', last='shah')
        assert len(self.cache) == 3
        assert value3 != value1
        assert value3 != value2
        assert self.expensive_method(first='raj', last='shah') == value3
        assert len(self.cache) == 3

        value4 = self.expensive_method(last='shah', first='raj')
        assert len(self.cache) == 3
        assert value4 == value3

        value5 = self.expensive_method('raj', last='shah')
        assert len(self.cache) == 4
        assert value5 != value1
        assert value5 != value2
        assert value5 != value3
        assert value5 != value4
        assert self.expensive_method('raj', last='shah') == value5
        assert len(self.cache) == 4

    def test_expiration(self):
        self.expensive_method()
        assert self.redis.ttl(self._KEY) == _DEFAULT_TIMEOUT
        time.sleep(1)
        assert self.redis.ttl(self._KEY) == _DEFAULT_TIMEOUT - 1

        self.expensive_method()
        assert self.redis.ttl(self._KEY) == _DEFAULT_TIMEOUT
        time.sleep(1)
        assert self.redis.ttl(self._KEY) == _DEFAULT_TIMEOUT - 1

        self.expensive_method('raj')
        assert self.redis.ttl(self._KEY) == _DEFAULT_TIMEOUT

    def test_wrapped(self):
        assert self.expensive_method() == self.expensive_method()
        assert self.expensive_method() != self.expensive_method.__wrapped__()
