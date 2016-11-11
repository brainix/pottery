#-----------------------------------------------------------------------------#
#   test_base.py                                                              #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import unittest.mock

from redis import Redis
from redis import WatchError
from redis.client import Pipeline

from pottery import RandomKeyError
from pottery import RedisDict
from pottery import TooManyTriesError
from tests.base import TestCase



class _BaseTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.redis = Redis()
        self.raj = RedisDict(key='pottery:raj', hobby='music', vegetarian=True)
        self.nilika = RedisDict(key='pottery:nilika', hobby='music', vegetarian=True)
        self.luvh = RedisDict(key='luvh', hobby='bullying', vegetarian=False)

    def tearDown(self):
        self.redis.delete('luvh')
        super().tearDown()



class CommonTests(_BaseTestCase):
    def test_del(self):
        with unittest.mock.patch.object(self.luvh.redis, 'delete') as delete:
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

    def test_randomkeyerror(self):
        with self.assertRaises(RandomKeyError), \
             unittest.mock.patch.object(self.raj.redis, 'exists') as exists:
            exists.return_value = True
            self.raj._random_key()



class PipelinedTests(_BaseTestCase):
    def test_toomanytrieserror(self):
        with self.assertRaises(TooManyTriesError), \
             unittest.mock.patch.object(Pipeline, 'execute') as execute:
            execute.side_effect = WatchError
            self.raj.update({'job': 'software'})



class IterableTests(TestCase):
    def test_iter(self):
        garbage = RedisDict()
        for num in range(1024):
            garbage[num] = num
        assert set(iter(garbage)) == set(range(1024))
