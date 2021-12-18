# --------------------------------------------------------------------------- #
#   test_hyper.py                                                             #
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


import uuid

from redis import Redis

from pottery import HyperLogLog
from tests.base import TestCase


class HyperLogLogTests(TestCase):
    _KEY = 'hll'

    def test_init_without_iterable(self):
        hll = HyperLogLog(redis=self.redis)
        assert len(hll) == 0

    def test_init_with_iterable(self):
        hll = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        assert len(hll) == 4

    def test_add(self):
        hll = HyperLogLog(redis=self.redis)
        hll.add('foo')
        assert len(hll) == 1

        hll.add('bar')
        assert len(hll) == 2

        hll.add('zap')
        assert len(hll) == 3

        hll.add('a')
        assert len(hll) == 4

        hll.add('a')
        assert len(hll) == 4

        hll.add('b')
        assert len(hll) == 5

        hll.add('c')
        assert len(hll) == 6

        hll.add('foo')
        assert len(hll) == 6

    def test_update(self):
        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        hll2 = HyperLogLog({'a', 'b', 'c', 'foo'}, redis=self.redis)
        hll1.update(hll2)
        assert len(hll1) == 6

        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        hll1.update({'b', 'c', 'd', 'foo'})
        assert len(hll1) == 7

        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        hll1.update(hll2, {'b', 'c', 'd', 'baz'})
        assert len(hll1) == 8

    def test_update_different_redis_instances(self):
        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        hll2 = HyperLogLog({'a', 'b', 'c', 'foo'}, redis=Redis())
        with self.assertRaises(RuntimeError):
            hll1.update(hll2)

    def test_union(self):
        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        hll2 = HyperLogLog({'a', 'b', 'c', 'foo'}, redis=self.redis)
        assert len(hll1.union(hll2, redis=self.redis)) == 6
        assert len(hll1.union({'b', 'c', 'd', 'foo'}, redis=self.redis)) == 7
        assert len(hll1.union(hll2, {'b', 'c', 'd', 'baz'}, redis=self.redis)) == 8

    def test_contains(self):
        metasyntactic_variables = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        for metasyntactic_variable in {'foo', 'bar'}:
            with self.subTest(metasyntactic_variable=metasyntactic_variable):
                assert metasyntactic_variable in metasyntactic_variables

        for metasyntactic_variable in {'baz', 'qux'}:
            with self.subTest(metasyntactic_variable=metasyntactic_variable):
                assert metasyntactic_variable not in metasyntactic_variables

    def test_contains_many_metasyntactic_variables(self):
        metasyntactic_variables = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        contains_many = metasyntactic_variables.contains_many('foo', 'bar', 'baz', 'quz')
        assert tuple(contains_many) == (True, True, False, False)

    def test_contains_many_uuids(self):
        NUM_ELEMENTS = 5000
        uuid_list = []
        for _ in range(NUM_ELEMENTS):
            uuid_ = str(uuid.uuid4())
            uuid_list.append(uuid_)
        uuid_hll = HyperLogLog(uuid_list, redis=self.redis)
        num_contained = sum(uuid_hll.contains_many(*uuid_list))
        assert num_contained == NUM_ELEMENTS

    def test_membership_for_non_jsonifyable_element(self):
        hll = HyperLogLog(redis=self.redis, key=self._KEY)
        assert not BaseException in hll

    def test_repr(self):
        'Test HyperLogLog.__repr__()'
        hll = HyperLogLog(
            {'foo', 'bar', 'zap', 'a'},
            redis=self.redis,
            key=self._KEY,
        )
        assert repr(hll) == f'<HyperLogLog key={self._KEY} len=4>'
