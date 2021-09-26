# --------------------------------------------------------------------------- #
#   test_nextid.py                                                            #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
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
'Distributed Redis-powered monotonically increasing ID generator tests.'


import unittest.mock

from redis.client import Script
from redis.exceptions import TimeoutError

from pottery import NextId
from pottery import QuorumIsImpossible
from pottery import QuorumNotAchieved
from tests.base import TestCase  # type: ignore


class NextIdTests(TestCase):
    'Distributed Redis-powered monotonically increasing ID generator tests.'

    def setUp(self):
        super().setUp()
        self.redis.delete('nextid:current')
        self.ids = NextId(masters={self.redis})
        for master in self.ids.masters:
            master.set(self.ids.key, 0)

    def test_nextid(self):
        for id_ in range(1, 10):
            with self.subTest(id_=id_):
                assert next(self.ids) == id_

    def test_iter(self):
        assert iter(self.ids) is self.ids

    def test_reset(self):
        next(self.ids)
        assert self.redis.exists(self.ids.key)

        self.ids.reset()
        assert not self.redis.exists(self.ids.key)

    def test_repr(self):
        assert repr(self.ids) == '<NextId key=nextid:current>'

    def test_slots(self):
        with self.assertRaises(AttributeError):
            self.ids.__dict__

    def test_next_quorumnotachieved(self):
        with self.assertRaises(QuorumNotAchieved), \
             unittest.mock.patch.object(
                 next(iter(self.ids.masters)),
                 'get',
             ) as get:
            get.side_effect = TimeoutError
            next(self.ids)

        with self.assertRaises(QuorumNotAchieved), \
             unittest.mock.patch.object(Script, '__call__') as __call__:
            __call__.side_effect = TimeoutError
            next(self.ids)

    def test_next_quorumisimpossible(self):
        self.ids = NextId(masters={self.redis}, raise_on_redis_errors=True)

        with self.assertRaises(QuorumIsImpossible), \
             unittest.mock.patch.object(
                 next(iter(self.ids.masters)),
                 'get',
             ) as get:
            get.side_effect = TimeoutError
            next(self.ids)

        with self.assertRaises(QuorumIsImpossible), \
             unittest.mock.patch.object(Script, '__call__') as __call__:
            __call__.side_effect = TimeoutError
            next(self.ids)

    def test_reset_quorumnotachieved(self):
        with self.assertRaises(QuorumNotAchieved), \
             unittest.mock.patch.object(
                 next(iter(self.ids.masters)),
                 'delete',
             ) as delete:
            delete.side_effect = TimeoutError
            self.ids.reset()

    def test_reset_quorumisimpossible(self):
        self.ids = NextId(masters={self.redis}, raise_on_redis_errors=True)

        with self.assertRaises(QuorumIsImpossible), \
             unittest.mock.patch.object(
                 next(iter(self.ids.masters)),
                 'delete',
             ) as delete:
            delete.side_effect = TimeoutError
            self.ids.reset()
