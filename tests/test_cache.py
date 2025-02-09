# --------------------------------------------------------------------------- #
#   test_cache.py                                                             #
#                                                                             #
#   Copyright © 2015-2025, Rajiv Bakulesh Shah, original author.              #
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


import collections
import concurrent.futures
import random
import time
import unittest.mock

import pytest
from redis import Redis

from pottery import redis_cache
from pottery.cache import _DEFAULT_TIMEOUT
from pottery.cache import CachedOrderedDict
from pottery.cache import CacheInfo


class TestCacheDecorator:
    KEY_EXPIRATION = 'expensive-method-expiration'
    KEY_NO_EXPIRATION = 'expensive-method-no-expiration'

    @pytest.fixture(autouse=True)
    def setup(self, redis: Redis) -> None:
        self.redis = redis

        def expensive_method(*args, **kwargs):
            'getrandbits(16) -> x.  Generates a 16-bit random int.'
            return random.getrandbits(16)

        self.expensive_method_expiration = redis_cache(
            redis=redis,
            key=self.KEY_EXPIRATION,
        )(expensive_method)

        self.expensive_method_no_expiration = redis_cache(
            redis=redis,
            key=self.KEY_NO_EXPIRATION,
            timeout=None,
        )(expensive_method)

        self.expensive_method_no_cache_kwargs = redis_cache()(expensive_method)

    def test_cache(self):
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=0,
            misses=0,
            maxsize=None,
            currsize=0,
        )

        value1 = self.expensive_method_expiration()
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=0,
            misses=1,
            maxsize=None,
            currsize=1,
        )
        assert self.expensive_method_expiration() == value1
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=1,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        value2 = self.expensive_method_expiration('raj')
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=1,
            misses=2,
            maxsize=None,
            currsize=2,
        )
        assert value2 != value1
        assert self.expensive_method_expiration('raj') == value2
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=2,
            misses=2,
            maxsize=None,
            currsize=2,
        )

        value3 = self.expensive_method_expiration(first='raj', last='shah')
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=2,
            misses=3,
            maxsize=None,
            currsize=3,
        )
        assert value3 != value1
        assert value3 != value2
        assert self.expensive_method_expiration(first='raj', last='shah') == value3
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=3,
            misses=3,
            maxsize=None,
            currsize=3,
        )

        value4 = self.expensive_method_expiration(last='shah', first='raj')
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=4,
            misses=3,
            maxsize=None,
            currsize=3,
        )
        assert value4 == value3

        value5 = self.expensive_method_expiration('raj', last='shah')
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=4,
            misses=4,
            maxsize=None,
            currsize=4,
        )
        assert value5 != value1
        assert value5 != value2
        assert value5 != value3
        assert value5 != value4
        assert self.expensive_method_expiration('raj', last='shah') == value5
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=5,
            misses=4,
            maxsize=None,
            currsize=4,
        )

    def test_expiration(self):
        self.expensive_method_expiration()
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT
        time.sleep(1)
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT - 1

        self.expensive_method_expiration()
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT
        time.sleep(1)
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT - 1

        self.expensive_method_expiration('raj')
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT

        self.expensive_method_expiration.__bypass__()
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT
        time.sleep(1)
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT - 1

        self.expensive_method_expiration.__bypass__()
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT
        time.sleep(1)
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT - 1

        self.expensive_method_expiration.__bypass__('raj')
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT

    def test_no_expiration(self):
        self.expensive_method_no_expiration()
        assert self.redis.ttl(self.KEY_NO_EXPIRATION) == -1

        self.expensive_method_no_expiration()
        assert self.redis.ttl(self.KEY_NO_EXPIRATION) == -1

        self.expensive_method_no_expiration('raj')
        assert self.redis.ttl(self.KEY_NO_EXPIRATION) == -1

        self.expensive_method_no_expiration.__bypass__()
        assert self.redis.ttl(self.KEY_NO_EXPIRATION) == -1

        self.expensive_method_no_expiration.__bypass__()
        assert self.redis.ttl(self.KEY_NO_EXPIRATION) == -1

        self.expensive_method_no_expiration.__bypass__('raj')
        assert self.redis.ttl(self.KEY_NO_EXPIRATION) == -1

    def test_wrapped(self):
        value1 = self.expensive_method_expiration()
        assert self.expensive_method_expiration() == value1
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=1,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        value2 = self.expensive_method_expiration.__wrapped__()
        assert value2 != value1
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=1,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        assert self.expensive_method_expiration() == value1
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=2,
            misses=1,
            maxsize=None,
            currsize=1,
        )

    @unittest.mock.patch('random.getrandbits')
    def test_bypass(self, getrandbits):
        getrandbits.return_value = 5

        self.expensive_method_expiration()
        assert getrandbits.call_count == 1
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=0,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        self.expensive_method_expiration()
        assert getrandbits.call_count == 1
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=1,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        self.expensive_method_expiration.__bypass__()
        assert getrandbits.call_count == 2
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=1,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        self.expensive_method_expiration()
        assert getrandbits.call_count == 2
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=2,
            misses=1,
            maxsize=None,
            currsize=1,
        )

    def test_cache_clear(self):
        self.expensive_method_expiration()
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=0,
            misses=1,
            maxsize=None,
            currsize=1,
        )

        self.expensive_method_expiration.cache_clear()
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=0,
            misses=0,
            maxsize=None,
            currsize=0,
        )

        self.expensive_method_expiration()
        self.expensive_method_expiration()
        self.expensive_method_expiration('raj')
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=1,
            misses=2,
            maxsize=None,
            currsize=2,
        )

        self.expensive_method_expiration.cache_clear()
        assert self.expensive_method_expiration.cache_info() == CacheInfo(
            hits=0,
            misses=0,
            maxsize=None,
            currsize=0,
        )

    def test_no_cache_kwargs(self):
        assert self.expensive_method_no_cache_kwargs.cache_info() == CacheInfo(
            hits=0,
            misses=0,
            maxsize=None,
            currsize=0,
        )


class TestCachedOrderedDict:
    KEY_EXPIRATION = 'cached-ordereddict-expiration'
    KEY_NO_EXPIRATION = 'cached-ordereddict-no-expiration'

    @pytest.fixture(autouse=True)
    def setup(self, redis: Redis) -> None:
        self.redis = redis

        # Populate the cache with three hits:
        for redis_key, timeout in {
            (self.KEY_EXPIRATION, _DEFAULT_TIMEOUT),
            (self.KEY_NO_EXPIRATION, None),
        }:
            cache = CachedOrderedDict(
                redis_client=redis,
                redis_key=redis_key,
                dict_keys=('hit1', 'hit2', 'hit3'),
                timeout=timeout,
            )
            cache['hit1'] = 'value1'
            cache['hit2'] = 'value2'
            cache['hit3'] = 'value3'

        # Instantiate the cache again with the three hits and three misses:
        self.cache_expiration = CachedOrderedDict(
            redis_client=redis,
            redis_key=self.KEY_EXPIRATION,
            dict_keys=('hit1', 'miss1', 'hit2', 'miss2', 'hit3', 'miss3'),
        )
        self.cache_no_expiration = CachedOrderedDict(
            redis_client=redis,
            redis_key=self.KEY_NO_EXPIRATION,
            dict_keys=('hit1', 'miss1', 'hit2', 'miss2', 'hit3', 'miss3'),
            timeout=None,
        )

    def test_setitem(self):
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
        ))
        assert self.cache_expiration._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
        }
        assert self.cache_expiration.misses() == {'miss1', 'miss2', 'miss3'}

        self.cache_expiration['hit4'] = 'value4'
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            ('hit4', 'value4'),
        ))
        assert self.cache_expiration._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
            'hit4': 'value4',
        }
        assert self.cache_expiration.misses() == {'miss1', 'miss2', 'miss3'}

        self.cache_expiration['miss1'] = 'value1'
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', 'value1'),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            ('hit4', 'value4'),
        ))
        assert self.cache_expiration._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
            'hit4': 'value4',
            'miss1': 'value1',
        }
        assert self.cache_expiration.misses() == {'miss2', 'miss3'}

    @pytest.mark.parametrize('default', ('rajiv', 'raj'))
    def test_setdefault(self, default):
        'Ensure setdefault() sets the key iff the key does not exist.'
        self.cache_expiration.setdefault('first', default=default)
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            ('first', default),
        ))
        assert self.cache_expiration._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
            'first': default,
        }
        assert self.cache_expiration.misses() == {
            'miss1',
            'miss2',
            'miss3',
        }

        self.cache_expiration.setdefault('miss1', default='value1')
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', 'value1'),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            ('first', default),
        ))
        assert self.cache_expiration._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
            'first': default,
            'miss1': 'value1',
        }
        assert self.cache_expiration.misses() == {'miss2', 'miss3'}

    def test_update(self):
        self.cache_expiration.update()
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
        ))
        assert self.cache_expiration._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
        }
        assert self.cache_expiration.misses() == {'miss1', 'miss2', 'miss3'}

        self.cache_expiration.update((
            ('miss1', 'value1'),
            ('miss2', 'value2'),
            ('hit4', 'value4'),
            ('hit5', 'value5'),
        ))
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', 'value1'),
            ('hit2', 'value2'),
            ('miss2', 'value2'),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            ('hit4', 'value4'),
            ('hit5', 'value5'),
        ))
        assert self.cache_expiration._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
            'miss1': 'value1',
            'miss2': 'value2',
            'hit4': 'value4',
            'hit5': 'value5',
        }
        assert self.cache_expiration.misses() == {'miss3'}

        self.cache_expiration.update({'miss3': CachedOrderedDict._SENTINEL})
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', 'value1'),
            ('hit2', 'value2'),
            ('miss2', 'value2'),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            ('hit4', 'value4'),
            ('hit5', 'value5'),
        ))
        assert self.cache_expiration._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
            'miss1': 'value1',
            'miss2': 'value2',
            'hit4': 'value4',
            'hit5': 'value5',
        }
        assert self.cache_expiration.misses() == {'miss3'}

        self.cache_expiration.update(miss3='value3')
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', 'value1'),
            ('hit2', 'value2'),
            ('miss2', 'value2'),
            ('hit3', 'value3'),
            ('miss3', 'value3'),
            ('hit4', 'value4'),
            ('hit5', 'value5'),
        ))
        assert self.cache_expiration._cache == {
            'hit1': 'value1',
            'hit2': 'value2',
            'hit3': 'value3',
            'miss1': 'value1',
            'miss2': 'value2',
            'hit4': 'value4',
            'hit5': 'value5',
            'miss3': 'value3',
        }
        assert self.cache_expiration.misses() == set()

    def test_non_string_keys(self):
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
        ))

        self.cache_expiration[None] = None
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            (None, None),
        ))

        self.cache_expiration[False] = False
        self.cache_expiration[True] = True
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            (None, None),
            (False, False),
            (True, True),
        ))

        self.cache_expiration[0] = 0
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            (None, None),
            (False, False),
            (True, True),
            (0, 0),
        ))

        self.cache_expiration[0.0] = 0.0
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('miss2', CachedOrderedDict._SENTINEL),
            ('hit3', 'value3'),
            ('miss3', CachedOrderedDict._SENTINEL),
            (None, None),
            (False, False),
            (True, True),
            (0, 0),
            (0.0, 0.0),
        ))

    def test_no_keys(self):
        cache = CachedOrderedDict(redis_client=self.redis)
        assert cache == {}
        assert cache.misses() == set()

    def test_expiration(self):
        futures = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for test_method in {
                self._test_expiration,
                self._test_no_expiration,
            }:
                future = executor.submit(test_method)
                futures.append(future)
        for future in futures:
            future.result()

    def _test_expiration(self):
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT
        time.sleep(1)
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT - 1
        self.cache_expiration['hit4'] = 'value4'
        assert self.redis.ttl(self.KEY_EXPIRATION) == _DEFAULT_TIMEOUT

    def _test_no_expiration(self):
        assert self.redis.ttl(self.KEY_NO_EXPIRATION) == -1
        time.sleep(1)
        assert self.redis.ttl(self.KEY_NO_EXPIRATION) == -1
        self.cache_no_expiration['hit4'] = 'value4'
        assert self.redis.ttl(self.KEY_NO_EXPIRATION) == -1

    def test_set_sentinel(self):
        self.cache_expiration = CachedOrderedDict(
            redis_client=self.redis,
            redis_key=self.KEY_EXPIRATION,
            dict_keys=('hit1', 'miss1', 'hit2', 'hit3'),
        )
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('hit3', 'value3'),
        ))
        self.cache_expiration['miss1'] = CachedOrderedDict._SENTINEL
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('hit3', 'value3'),
        ))
        self.cache_expiration['miss2'] = CachedOrderedDict._SENTINEL
        assert self.cache_expiration == collections.OrderedDict((
            ('hit1', 'value1'),
            ('miss1', CachedOrderedDict._SENTINEL),
            ('hit2', 'value2'),
            ('hit3', 'value3'),
            ('miss2', CachedOrderedDict._SENTINEL),
        ))
