# --------------------------------------------------------------------------- #
#   test_set.py                                                               #
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
'''These tests come from these examples:
    https://docs.python.org/3/tutorial/datastructures.html#sets
'''


import uuid

import pytest
from redis import Redis

from pottery import KeyExistsError
from pottery import RedisSet


def test_init(redis: Redis) -> None:
    set_ = RedisSet(redis=redis)
    assert set_ == set()


def test_keyexistserror(redis: Redis) -> None:
    fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}
    basket = RedisSet(fruits, redis=redis, key='pottery:basket')
    basket  # Workaround for Pyflakes.  :-(
    with pytest.raises(KeyExistsError):
        RedisSet(fruits, redis=redis, key='pottery:basket')


def test_basic_usage(redis: Redis) -> None:
    fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}
    basket = RedisSet(fruits, redis=redis)
    assert basket == {'orange', 'banana', 'pear', 'apple'}
    assert 'orange' in basket
    assert not 'crabgrass' in basket


def test_contains_many_metasyntactic_variables(redis: Redis) -> None:
    metasyntactic_variables = RedisSet({'foo', 'bar', 'zap', 'a'}, redis=redis)
    contains_many = metasyntactic_variables.contains_many('foo', 'bar', object(), 'baz', 'quz')  # type: ignore
    assert tuple(contains_many) == (True, True, False, False, False)


def test_contains_many_uuids(redis: Redis) -> None:
    NUM_ELEMENTS = 5000
    known_uuids, unknown_uuids = [], []
    generate_uuid = lambda: str(uuid.uuid4())  # NoQA: E731
    for _ in range(NUM_ELEMENTS):
        known_uuids.append(generate_uuid())  # type: ignore
        unknown_uuids.append(generate_uuid())  # type: ignore
    uuid_set = RedisSet(known_uuids, redis=redis)
    num_known_contained = sum(uuid_set.contains_many(*known_uuids))
    num_unknown_contained = sum(uuid_set.contains_many(*unknown_uuids))
    assert num_known_contained == NUM_ELEMENTS
    assert num_unknown_contained == 0


def test_add(redis: Redis) -> None:
    fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}
    basket = RedisSet(fruits, redis=redis)
    basket.add('tomato')
    assert basket == {'apple', 'orange', 'apple', 'pear', 'orange', 'banana', 'tomato'}


def test_discard(redis: Redis) -> None:
    fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana', 'tomato'}
    basket = RedisSet(fruits, redis=redis)
    basket.discard('tomato')
    assert basket == {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}


def test_repr(redis: Redis) -> None:
    basket = RedisSet({'apple'}, redis=redis)
    assert repr(basket) == "RedisSet{'apple'}"

    basket = RedisSet({'apple', 'orange'}, redis=redis)
    assert repr(basket) in {
        "RedisSet{'apple', 'orange'}",
        "RedisSet{'orange', 'apple'}",
    }


def test_pop(redis: Redis) -> None:
    fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}
    basket = RedisSet(fruits, redis=redis)
    for _ in range(len(fruits)):
        fruit = basket.pop()
        assert fruit in fruits
        fruits.discard(fruit)  # type: ignore

    assert not fruits
    assert not basket
    with pytest.raises(KeyError):
        basket.pop()


def test_remove(redis: Redis) -> None:
    basket = RedisSet({'apple', 'orange'}, redis=redis)
    basket.remove('orange')
    assert basket == {'apple'}

    basket.remove('apple')
    assert basket == set()

    with pytest.raises(KeyError):
        basket.remove('apple')


def test_set_operations(redis: Redis) -> None:
    a = RedisSet('abracadabra', redis=redis)
    b = RedisSet('alacazam', redis=redis)
    assert a == {'a', 'r', 'b', 'c', 'd'}
    assert a - b == {'r', 'd', 'b'}
    assert isinstance(a - b, RedisSet)
    assert a | b == {'a', 'c', 'r', 'd', 'b', 'm', 'z', 'l'}
    assert isinstance(a | b, RedisSet)
    assert a & b == {'a', 'c'}
    assert isinstance(a & b, RedisSet)
    assert a ^ b == {'r', 'd', 'b', 'm', 'z', 'l'}
    assert isinstance(a ^ b, RedisSet)


def test_isdisjoint(redis: Redis) -> None:
    a = RedisSet('abc', redis=redis)
    b = RedisSet('cde', redis=redis)
    assert not a.isdisjoint(b)
    c = RedisSet('def', redis=redis)
    assert a.isdisjoint(c)

    other_url = 'redis://127.0.0.1:6379/'
    other_redis = Redis.from_url(other_url, socket_timeout=1)
    b = RedisSet('cde', redis=other_redis)
    assert not a.isdisjoint(b)
    c = RedisSet('def', redis=other_redis)
    assert a.isdisjoint(c)

    d = {'c', 'd', 'e'}
    assert not a.isdisjoint(d)
    e = {'d', 'e', 'f'}
    assert a.isdisjoint(e)


def test_issubset(redis: Redis) -> None:
    a = RedisSet('abc', redis=redis)
    b = RedisSet('abc', redis=redis)
    assert a.issubset(tuple(b))
    assert b.issubset(tuple(a))
    assert a.issubset(b)
    assert b.issubset(a)
    assert a <= b
    assert b <= a
    assert not a < b
    assert not b < a
    c = RedisSet('abcd', redis=redis)
    assert a.issubset(c)
    assert not c.issubset(a)
    assert a <= c
    assert not c <= a
    assert a < c
    assert not c < a
    d = RedisSet('def', redis=redis)
    assert not a.issubset(d)
    assert not d.issubset(a)
    assert not a <= d
    assert not d <= a
    assert not a < d
    assert not d < a


def test_issuperset(redis: Redis) -> None:
    a = RedisSet('abc', redis=redis)
    b = RedisSet('abc', redis=redis)
    assert a.issuperset(tuple(b))
    assert b.issuperset(tuple(a))
    assert a.issuperset(b)
    assert b.issuperset(a)
    assert a >= b
    assert b >= a
    assert not a > b
    assert not b > a
    c = RedisSet('abcd', redis=redis)
    assert not a.issuperset(c)
    assert c.issuperset(a)
    assert not a >= c
    assert c >= a
    assert not a > c
    assert c > a
    d = RedisSet('def', redis=redis)
    assert not a.issuperset(d)
    assert not d.issuperset(a)
    assert not a >= d
    assert not d >= a
    assert not a > d
    assert not d > a


def test_union(redis: Redis) -> None:
    a = RedisSet('abc', redis=redis)
    b = RedisSet('cde', redis=redis)
    assert a.union(b) == {'a', 'b', 'c', 'd', 'e'}

    other_url = 'redis://127.0.0.1:6379/'
    other_redis = Redis.from_url(other_url, socket_timeout=1)
    a = RedisSet('abc', redis=redis)
    b = RedisSet('cde', redis=other_redis)
    assert a.union(b) == {'a', 'b', 'c', 'd', 'e'}

    a = RedisSet(redis=redis)
    b = {'a', 'b', 'c'}  # type: ignore
    assert a.union(b) == {'a', 'b', 'c'}


def test_intersection(redis: Redis) -> None:
    a = RedisSet('abc', redis=redis)
    b = RedisSet('cde', redis=redis)
    assert a.intersection(b) == {'c'}
    c = RedisSet('def', redis=redis)
    assert a.intersection(c) == set()

    other_url = 'redis://127.0.0.1:6379/'
    other_redis = Redis.from_url(other_url, socket_timeout=1)
    b = RedisSet('cde', redis=other_redis)
    assert a.intersection(b) == {'c'}
    c = RedisSet('def', redis=other_redis)
    assert a.intersection(c) == set()

    d = {'c', 'd', 'e'}
    assert a.intersection(d) == {'c'}
    e = {'d', 'e', 'f'}
    assert a.intersection(e) == set()


def test_difference(redis: Redis) -> None:
    a = RedisSet('abcd', redis=redis)
    b = RedisSet('c', redis=redis)
    c = RedisSet('ace', redis=redis)
    assert a.difference(b, c) == {'b', 'd'}
    assert a - b - c == {'b', 'd'}

    b = {'c'}  # type: ignore
    c = {'a', 'c', 'e'}  # type: ignore
    assert a.difference(b, c) == {'b', 'd'}
    assert a - b - c == {'b', 'd'}


def test_update_with_no_arguments(redis: Redis) -> None:
    s = RedisSet(redis=redis)
    s.update()
    assert s == set()

    s = RedisSet('abcd', redis=redis)
    s.update()
    assert s == {'a', 'b', 'c', 'd'}


def test_update_with_empty_set(redis: Redis) -> None:
    metasyntactic_variables = RedisSet(redis=redis)
    metasyntactic_variables.update(set())
    assert metasyntactic_variables == set()

    metasyntactic_variables = RedisSet({'foo', 'bar', 'baz'}, redis=redis)
    metasyntactic_variables.update(set())
    assert metasyntactic_variables == {'foo', 'bar', 'baz'}


def test_update_with_redisset(redis: Redis) -> None:
    metasyntactic_variables_1 = RedisSet(redis=redis)
    metasyntactic_variables_2 = RedisSet({'foo', 'bar', 'baz'}, redis=redis)
    metasyntactic_variables_1.update(metasyntactic_variables_2)
    assert metasyntactic_variables_1 == {'foo', 'bar', 'baz'}
    metasyntactic_variables_3 = RedisSet({'qux'}, redis=redis)
    metasyntactic_variables_1.update(metasyntactic_variables_3)
    assert metasyntactic_variables_1 == {'foo', 'bar', 'baz', 'qux'}


def test_update_with_set(redis: Redis) -> None:
    metasyntactic_variables = RedisSet(redis=redis)
    metasyntactic_variables.update({'foo', 'bar', 'baz'})
    assert metasyntactic_variables == {'foo', 'bar', 'baz'}
    metasyntactic_variables.update({'qux'})
    assert metasyntactic_variables == {'foo', 'bar', 'baz', 'qux'}


def test_update_with_range(redis: Redis) -> None:
    ramanujans_friends = RedisSet(redis=redis)
    ramanujans_friends.update(range(10))
    assert ramanujans_friends == {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}


def test_update_with_set_and_range(redis: Redis) -> None:
    silliness = RedisSet(redis=redis)
    silliness.update({'foo', 'bar', 'baz'}, range(5))
    assert silliness == {'foo', 'bar', 'baz', 0, 1, 2, 3, 4}
    silliness.update({'qux'}, range(6))
    assert silliness == {'foo', 'bar', 'baz', 'qux', 0, 1, 2, 3, 4, 5}


def test_difference_update_with_empty_set(redis: Redis) -> None:
    ramanujans_friends = RedisSet(redis=redis)
    ramanujans_friends.difference_update(set())
    assert ramanujans_friends == set()

    ramanujans_friends = RedisSet(range(10), redis=redis)
    ramanujans_friends.difference_update(set())
    assert ramanujans_friends == {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}


def test_difference_update_with_redisset(redis: Redis) -> None:
    ramanujans_friends_1 = RedisSet(redis=redis)
    ramanujans_friends_2 = RedisSet({5, 6, 7, 8, 9}, redis=redis)
    ramanujans_friends_1.difference_update(ramanujans_friends_2)
    assert ramanujans_friends_1 == set()

    ramanujans_friends_1 = RedisSet(range(10), redis=redis)
    ramanujans_friends_1.difference_update(ramanujans_friends_2)
    assert ramanujans_friends_1 == {0, 1, 2, 3, 4}


def test_difference_update_with_set(redis: Redis) -> None:
    ramanujans_friends = RedisSet(redis=redis)
    ramanujans_friends.difference_update({5, 6, 7, 8, 9})
    assert ramanujans_friends == set()

    ramanujans_friends = RedisSet(range(10), redis=redis)
    ramanujans_friends.difference_update({5, 6, 7, 8, 9})
    assert ramanujans_friends == {0, 1, 2, 3, 4}


def test_difference_update_with_range(redis: Redis) -> None:
    ramanujans_friends = RedisSet(redis=redis)
    ramanujans_friends.difference_update(range(5))
    assert ramanujans_friends == set()

    ramanujans_friends = RedisSet(range(10), redis=redis)
    ramanujans_friends.difference_update(range(5))
    assert ramanujans_friends == {5, 6, 7, 8, 9}


def test_difference_update_with_range_and_set(redis: Redis) -> None:
    ramanujans_friends = RedisSet(redis=redis)
    ramanujans_friends.difference_update(range(4), {6, 7, 8, 9})
    assert ramanujans_friends == set()

    ramanujans_friends = RedisSet(range(10), redis=redis)
    ramanujans_friends.difference_update(range(4), {6, 7, 8, 9})
    assert ramanujans_friends == {4, 5}


def test_membership_for_non_jsonifyable_element(redis: Redis) -> None:
    redis_set = RedisSet(redis=redis)
    assert not BaseException in redis_set


def test_populate_with_empty_iterable(redis: Redis) -> None:
    redis_set = RedisSet(redis=redis)
    with redis_set._watch() as pipeline:
        redis_set._populate(pipeline, tuple())
    assert redis_set == set()
