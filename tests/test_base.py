# --------------------------------------------------------------------------- #
#   test_base.py                                                              #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import gc
import unittest.mock

from pottery import RandomKeyError
from pottery import RedisDict
from pottery.base import random_key
from tests.base import TestCase  # type: ignore


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
        with unittest.mock.patch.object(self.redis, 'delete') as delete:
            del self.raj
            delete.assert_called_with('pottery:raj')
            delete.reset_mock()

            del self.nilika
            delete.assert_called_with('pottery:nilika')
            delete.reset_mock()

            del self.luvh
            delete.assert_not_called()

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


class IterableTests(TestCase):
    def test_iter(self):
        garbage = RedisDict()
        for num in range(1024):
            garbage[num] = num
        assert set(iter(garbage)) == set(range(1024))
