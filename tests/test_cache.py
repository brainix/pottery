#-----------------------------------------------------------------------------#
#   test_cache.py                                                             #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections
import random
import time
import unittest

from pottery import redis_cache
from pottery.cache import _DEFAULT_TIMEOUT
from pottery.cache import CachedOrderedDict
from pottery.cache import CacheInfo
from tests.base import TestCase



class CacheDecoratorTests(TestCase):
    _KEY = '{}expensive-method'.format(TestCase._TEST_KEY_PREFIX)

    def setUp(self):
        super().setUp()
        self.expensive_method.cache_clear()

    def tearDown(self):
        self.expensive_method.cache_clear()
        super().tearDown()

    @staticmethod
    @redis_cache(key=_KEY)
    def expensive_method(*args, **kwargs):
        'getrandbits(16) -> x.  Generates a 16-bit random int.'
        return random.getrandbits(16)

    def test_cache(self):
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=0,
            misses=0,
            maxsize=None,
            currsize=0,
        )

        value1 = self.expensive_method()
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=0,
            misses=1,
            maxsize=None,
            currsize=1,
        )
        assert self.expensive_method() == value1
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=1,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        value2 = self.expensive_method('raj')
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=1,
            misses=2,
            maxsize=None,
            currsize=2,
        )
        assert value2 != value1
        assert self.expensive_method('raj') == value2
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=2,
            misses=2,
            maxsize=None,
            currsize=2,
        )

        value3 = self.expensive_method(first='raj', last='shah')
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=2,
            misses=3,
            maxsize=None,
            currsize=3,
        )
        assert value3 != value1
        assert value3 != value2
        assert self.expensive_method(first='raj', last='shah') == value3
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=3,
            misses=3,
            maxsize=None,
            currsize=3,
        )

        value4 = self.expensive_method(last='shah', first='raj')
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=4,
            misses=3,
            maxsize=None,
            currsize=3,
        )
        assert value4 == value3

        value5 = self.expensive_method('raj', last='shah')
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=4,
            misses=4,
            maxsize=None,
            currsize=4,
        )
        assert value5 != value1
        assert value5 != value2
        assert value5 != value3
        assert value5 != value4
        assert self.expensive_method('raj', last='shah') == value5
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=5,
            misses=4,
            maxsize=None,
            currsize=4,
        )

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
        value1 = self.expensive_method()
        assert self.expensive_method() == value1
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=1,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        value2 = self.expensive_method.__wrapped__()
        assert value2 != value1
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=1,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        assert self.expensive_method() == value1
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=2,
            misses=1,
            maxsize=None,
            currsize=1,
        )

    @unittest.mock.patch('random.getrandbits')
    def test_bypass(self, getrandbits):
        getrandbits.return_value = 5

        self.expensive_method()
        assert getrandbits.call_count == 1
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=0,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        self.expensive_method()
        assert getrandbits.call_count == 1
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=1,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        self.expensive_method.__bypass__()
        assert getrandbits.call_count == 2
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=1,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        self.expensive_method()
        assert getrandbits.call_count == 2
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=2,
            misses=1,
            maxsize=None,
            currsize=1,
        )

    def test_cache_clear(self):
        self.expensive_method()
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=0,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        self.expensive_method.cache_clear()
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=0,
            misses=0,
            maxsize=None,
            currsize=0,
        )

        self.expensive_method()
        self.expensive_method()
        self.expensive_method('raj')
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=1,
            misses=2,
            maxsize=None,
            currsize=2,
        )

        self.expensive_method.cache_clear()
        assert self.expensive_method.cache_info() == CacheInfo(
            hits=0,
            misses=0,
            maxsize=None,
            currsize=0,
        )



class CachedOrderedDictTests(TestCase):
    _KEY = '{}cached-ordereddict'.format(TestCase._TEST_KEY_PREFIX)

    def setUp(self):
        super().setUp()

        # Populate the cache with three hits:
        with CachedOrderedDict(
            redis=self.redis,
            key=self._KEY,
            keys=('hit1', 'hit2', 'hit3'),
        ) as cache:
            cache['hit1'] = 'value1'
            cache['hit2'] = 'value2'
            cache['hit3'] = 'value3'

        # Instantiate the cache again with the three hits and three misses:
        self.cache = CachedOrderedDict(
            redis=self.redis,
            key=self._KEY,
            keys=('hit1', 'miss1', 'hit2', 'miss2', 'hit3', 'miss3'),
        )

    def test_setitem(self):
        assert self.cache == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
        ))
        assert self.cache._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
        }
        assert self.cache.misses() == {'miss1', 'miss2', 'miss3'}

        self.cache['hit4'] = 'value4'
        assert self.cache == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            ('hit4', 'value4'),
        ))
        assert self.cache._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
            'hit4': 'value4',
        }
        assert self.cache.misses() == {'miss1', 'miss2', 'miss3'}

        self.cache['miss1'] = 'value1'
        assert self.cache == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', 'value1'),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            ('hit4', 'value4'),
        ))
        assert self.cache._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
            'hit4': 'value4',
            'miss1': 'value1',
        }
        assert self.cache.misses() == {'miss2', 'miss3'}

    def test_setdefault(self):
        'Ensure setdefault() sets the key iff the key does not exist.'
        for default in ('rajiv', 'raj'):
            with self.subTest(default=default):
                self.cache.setdefault('first', default=default)
                assert self.cache == collections.OrderedDict((
                    ('hit1', 'value1'),
                    ('miss1', CachedOrderedDict._SENTINEL),
                    ('hit2', 'value2'),
                    ('miss2', CachedOrderedDict._SENTINEL),
                    ('hit3', 'value3'),
                    ('miss3', CachedOrderedDict._SENTINEL),
                    ('first', 'rajiv'),
                ))
                assert self.cache._cache == {
                    'hit1': 'value1',
                    'hit2': 'value2',
                    'hit3': 'value3',
                    'first': 'rajiv',
                }
                assert self.cache.misses() == {'miss1', 'miss2', 'miss3'}

        self.cache.setdefault('miss1', default='value1')
        assert self.cache == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', 'value1'),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            ('first', 'rajiv'),
        ))
        assert self.cache._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
            'first': 'rajiv',
            'miss1': 'value1',
        }
        assert self.cache.misses() == {'miss2', 'miss3'}

    def test_update(self):
        self.cache.update()
        assert self.cache == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
        ))
        assert self.cache._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
        }
        assert self.cache.misses() == {'miss1', 'miss2', 'miss3'}

        self.cache.update((
            ('miss1', 'value1'),
            ('miss2', 'value2'),
            ('hit4', 'value4'),
            ('hit5', 'value5'),
        ))
        assert self.cache == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', 'value1'),
            ('hit2', 'value2'),
            ('miss2', 'value2'),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            ('hit4', 'value4'),
            ('hit5', 'value5'),
        ))
        assert self.cache._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
            'miss1': 'value1',
            'miss2': 'value2',
            'hit4': 'value4',
            'hit5': 'value5',
        }
        assert self.cache.misses() == {'miss3'}
