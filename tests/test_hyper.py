# --------------------------------------------------------------------------- #
#   test_hyper.py                                                             #
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


import uuid

import pytest
from redis import Redis

from pottery import HyperLogLog


def test_init_without_iterable(redis: Redis) -> None:
    hll = HyperLogLog(redis=redis)
    assert len(hll) == 0


def test_init_with_iterable(redis: Redis) -> None:
    hll = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=redis)
    assert len(hll) == 4


def test_add(redis: Redis) -> None:
    hll = HyperLogLog(redis=redis)
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


def test_update(redis: Redis) -> None:
    hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=redis)
    hll2 = HyperLogLog({'a', 'b', 'c', 'foo'}, redis=redis)
    hll1.update(hll2)
    assert len(hll1) == 6

    hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=redis)
    hll1.update({'b', 'c', 'd', 'foo'})
    assert len(hll1) == 7

    hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=redis)
    hll1.update(hll2, {'b', 'c', 'd', 'baz'})
    assert len(hll1) == 8


def test_update_different_redis_instances(redis: Redis) -> None:
    hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=redis)
    hll2 = HyperLogLog(redis=Redis())
    with pytest.raises(RuntimeError):
        hll1.update(hll2)


def test_union(redis: Redis) -> None:
    hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=redis)
    hll2 = HyperLogLog({'a', 'b', 'c', 'foo'}, redis=redis)
    assert len(hll1.union(hll2, redis=redis)) == 6
    assert len(hll1.union({'b', 'c', 'd', 'foo'}, redis=redis)) == 7
    assert len(hll1.union(hll2, {'b', 'c', 'd', 'baz'}, redis=redis)) == 8


@pytest.mark.parametrize('metasyntactic_variable', ('foo', 'bar'))
def test_contains_metasyntactic_variables(redis: Redis, metasyntactic_variable: str) -> None:
    metasyntactic_variables = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=redis)
    assert metasyntactic_variable in metasyntactic_variables


@pytest.mark.parametrize('metasyntactic_variable', ('baz', 'qux'))
def test_doesnt_contain_metasyntactic_variables(redis: Redis, metasyntactic_variable: str) -> None:
    metasyntactic_variables = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=redis)
    assert metasyntactic_variable not in metasyntactic_variables


def test_contains_many_uuids(redis: Redis) -> None:
    NUM_ELEMENTS = 5000
    known_uuids, unknown_uuids = [], []
    generate_uuid = lambda: str(uuid.uuid4())  # NoQA: E731
    for _ in range(NUM_ELEMENTS):
        known_uuids.append(generate_uuid())  # type: ignore
        unknown_uuids.append(generate_uuid())  # type: ignore
    uuid_hll = HyperLogLog(known_uuids, redis=redis)
    num_known_contained = sum(uuid_hll.contains_many(*known_uuids))
    num_unknown_contained = sum(uuid_hll.contains_many(*unknown_uuids))
    assert num_known_contained == NUM_ELEMENTS
    assert num_unknown_contained <= NUM_ELEMENTS * 0.25, \
        f'{num_unknown_contained} is not <= {NUM_ELEMENTS * 0.25}'


def test_membership_for_non_jsonifyable_element(redis: Redis) -> None:
    hll = HyperLogLog(redis=redis)
    assert not BaseException in hll  # type: ignore


def test_repr(redis: Redis) -> None:
    'Test HyperLogLog.__repr__()'
    hll = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=redis, key='hll')
    assert repr(hll) == '<HyperLogLog key=hll len=4>'
