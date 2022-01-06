# --------------------------------------------------------------------------- #
#   test_base.py                                                              #
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


import gc
import unittest.mock

from pottery import RandomKeyError
from pottery import RedisDict
from pottery.base import random_key
from tests.base import TestCase


class RandomKeyTests(TestCase):
    def test_random_key_raises_typeerror_for_invalid_num_tries(self):
        with self.assertRaises(TypeError):
            random_key(redis=self.redis, num_tries=3.0)

    def test_random_key_raises_valueerror_for_invalid_num_tries(self):
        with self.assertRaises(ValueError):
            random_key(redis=self.redis, num_tries=-1)

    def test_random_key_raises_randomkeyerror_when_no_tries_left(self):
        with self.assertRaises(RandomKeyError), \
             unittest.mock.patch.object(self.redis, 'exists') as exists:
            exists.return_value = True
            random_key(redis=self.redis)


class _BaseTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.raj = RedisDict(
            redis=self.redis,
            key='pottery:raj',
            hobby='music',
            vegetarian=True,
        )
        self.nilika = RedisDict(
            redis=self.redis,
            key='pottery:nilika',
            hobby='music',
            vegetarian=True,
        )
        self.luvh = RedisDict(
            redis=self.redis,
            key='luvh',
            hobby='bullying',
            vegetarian=False,
        )


class CommonTests(_BaseTestCase):
    def test_out_of_scope(self):
        def scope():
            raj = RedisDict(redis=self.redis, hobby='music', vegetarian=True)
            assert self.redis.exists(raj.key)
            return raj.key

        key = scope()
        gc.collect()
        assert not self.redis.exists(key)

    def test_del(self):
        with unittest.mock.patch.object(self.redis, 'unlink') as unlink:
            del self.raj
            unlink.assert_called_with('pottery:raj')
            unlink.reset_mock()

            del self.nilika
            unlink.assert_called_with('pottery:nilika')
            unlink.reset_mock()

            del self.luvh
            unlink.assert_not_called()

    def test_eq(self):
        assert self.raj == self.raj
        assert self.raj == self.nilika
        assert self.raj == {'hobby': 'music', 'vegetarian': True}
        assert not self.raj == self.luvh
        assert not self.raj == None

    def test_ne(self):
        assert not self.raj != self.raj
        assert not self.raj != self.nilika
        assert not self.raj != {'hobby': 'music', 'vegetarian': True}
        assert self.raj != self.luvh
        assert self.raj != None

    def test_randomkeyerror_raised(self):
        with self.assertRaises(RandomKeyError), \
             unittest.mock.patch.object(self.raj.redis, 'exists') as exists:
            exists.return_value = True
            self.raj._random_key()

    def test_randomkeyerror_repr(self):
        with unittest.mock.patch.object(self.raj.redis, 'exists') as exists:
            exists.return_value = True
            try:
                self.raj._random_key()
            except RandomKeyError as wtf:
                assert repr(wtf) == f'RandomKeyError(redis=Redis<ConnectionPool<Connection<host=localhost,port=6379,db={self.redis_db}>>>)'
            else:  # pragma: no cover
                self.fail(msg='RandomKeyError not raised')

    def test_randomkeyerror_str(self):
        with unittest.mock.patch.object(self.raj.redis, 'exists') as exists:
            exists.return_value = True
            try:
                self.raj._random_key()
            except RandomKeyError as wtf:
                assert str(wtf) == f"redis=Redis<ConnectionPool<Connection<host=localhost,port=6379,db={self.redis_db}>>>"
            else:  # pragma: no cover
                self.fail(msg='RandomKeyError not raised')


class EncodableTests(TestCase):
    def test_decoded_responses(self):
        'Ensure that Pottery still works if the Redis client decodes responses.'
        tel = RedisDict(
            {'jack': 4098, 'sape': 4139},
            redis=self.redis_decoded_responses,
        )

        # Ensure that repr(tel) does not raise this exception:
        #
        # Traceback (most recent call last):
        #   File "/Users/rajiv.shah/Documents/Code/pottery/tests/test_base.py", line 139, in test_decoded_responses
        #     repr(tel)
        #   File "/Users/rajiv.shah/Documents/Code/pottery/pottery/dict.py", line 116, in __repr__
        #     dict_ = {self._decode(key): self._decode(value) for key, value in items}
        #   File "/Users/rajiv.shah/Documents/Code/pottery/pottery/dict.py", line 116, in <dictcomp>
        #     dict_ = {self._decode(key): self._decode(value) for key, value in items}
        #   File "/Users/rajiv.shah/Documents/Code/pottery/pottery/base.py", line 154, in _decode
        #     decoded: JSONTypes = json.loads(value.decode('utf-8'))
        # AttributeError: 'str' object has no attribute 'decode'
        repr(tel)


class IterableTests(TestCase):
    def test_iter(self):
        garbage = RedisDict()
        for num in range(1024):
            garbage[num] = num
        assert set(iter(garbage)) == set(range(1024))
