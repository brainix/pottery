# --------------------------------------------------------------------------- #
#   test_counter.py                                                           #
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
    https://docs.python.org/3/library/collections.html#counter-objects
'''


import collections
from typing import Counter

from redis import Redis

from pottery import RedisCounter
from pottery.base import _Common


def test_basic_usage(redis: Redis) -> None:
    c = RedisCounter(redis=redis)
    for word in ('red', 'blue', 'red', 'green', 'blue', 'blue'):
        c[word] += 1
    assert c == collections.Counter(blue=3, red=2, green=1)


def test_init(redis: Redis) -> None:
    c = RedisCounter(redis=redis)
    assert c == collections.Counter()


def test_init_with_iterable(redis: Redis) -> None:
    c = RedisCounter('gallahad', redis=redis)  # type: ignore
    assert c == collections.Counter(a=3, l=2, g=1, h=1, d=1)  # NoQA: E741


def test_init_with_mapping(redis: Redis) -> None:
    c = RedisCounter({'red': 4, 'blue': 2}, redis=redis)  # type: ignore
    assert c == collections.Counter(red=4, blue=2)


def test_init_with_kwargs(redis: Redis) -> None:
    c = RedisCounter(redis=redis, cats=4, dogs=8)
    assert c == collections.Counter(dogs=8, cats=4)


def test_missing_element_doesnt_raise_keyerror(redis: Redis) -> None:
    c = RedisCounter(('eggs', 'ham'), redis=redis)  # type: ignore
    assert c['bacon'] == 0


def test_setting_0_adds_item(redis: Redis) -> None:
    c = RedisCounter(('eggs', 'ham'), redis=redis)  # type: ignore
    assert set(c) == {'eggs', 'ham'}
    c['sausage'] = 0
    assert set(c) == {'eggs', 'ham', 'sausage'}


def test_setting_0_count_doesnt_remove_item(redis: Redis) -> None:
    c = RedisCounter(('eggs', 'ham'), redis=redis)  # type: ignore
    c['ham'] = 0
    assert set(c) == {'eggs', 'ham'}


def test_del_removes_item(redis: Redis) -> None:
    c = RedisCounter(('eggs', 'ham'), redis=redis)  # type: ignore
    c['sausage'] = 0
    assert set(c) == {'eggs', 'ham', 'sausage'}
    del c['sausage']
    assert set(c) == {'eggs', 'ham'}
    del c['ham']
    assert set(c) == {'eggs'}


def test_elements(redis: Redis) -> None:
    c = RedisCounter(redis=redis, a=4, b=2, c=0, d=-2)
    assert sorted(c.elements()) == ['a', 'a', 'a', 'a', 'b', 'b']


def test_most_common(redis: Redis) -> None:
    c = RedisCounter('abracadabra', redis=redis)  # type: ignore
    assert sorted(c.most_common(3)) == [('a', 5), ('b', 2), ('r', 2)]


def test_update_with_empty_dict(redis: Redis) -> None:
    c = RedisCounter(redis=redis, foo=1)
    c.update({})
    assert isinstance(c, RedisCounter)
    assert c == collections.Counter(foo=1)


def test_update_with_overlapping_dict(redis: Redis) -> None:
    c = RedisCounter(redis=redis, foo=1, bar=1)
    c.update({'bar': 1, 'baz': 3, 'qux': 4})
    assert isinstance(c, RedisCounter)
    assert c == collections.Counter(foo=1, bar=2, baz=3, qux=4)


def test_subtract(redis: Redis) -> None:
    c = RedisCounter(redis=redis, a=4, b=2, c=0, d=-2)
    d = RedisCounter(redis=redis, a=1, b=2, c=3, d=4)
    c.subtract(d)
    assert isinstance(c, RedisCounter)
    assert c == collections.Counter(a=3, b=0, c=-3, d=-6)


def test_repr(redis: Redis) -> None:
    'Test RedisCounter.__repr__()'
    c = RedisCounter(('eggs', 'ham'), redis=redis)  # type: ignore
    assert repr(c) in {
        "RedisCounter{'eggs': 1, 'ham': 1}",
        "RedisCounter{'ham': 1, 'eggs': 1}",
    }


def test_make_counter(redis: Redis) -> None:
    'Test RedisCounter._make_counter()'
    kwargs = {str(element): element for element in range(1000)}
    c = RedisCounter(redis=redis, **kwargs).to_counter()  # type: ignore
    assert c == collections.Counter(**kwargs)


def test_add(redis: Redis) -> None:
    'Test RedisCounter.__add__()'
    c = RedisCounter(redis=redis, a=3, b=1)
    d = RedisCounter(redis=redis, a=1, b=2)
    e = c + d
    assert isinstance(e, collections.Counter)
    assert e == collections.Counter(a=4, b=3)


def test_sub(redis: Redis) -> None:
    'Test RedisCounter.__sub__()'
    c = RedisCounter(redis=redis, a=3, b=1)
    d = RedisCounter(redis=redis, a=1, b=2)
    e = c - d
    assert isinstance(e, collections.Counter)
    assert e == collections.Counter(a=2)


def test_or(redis: Redis) -> None:
    'Test RedisCounter.__or__()'
    c = RedisCounter(redis=redis, a=3, b=1)
    d = RedisCounter(redis=redis, a=1, b=2)
    e = c | d
    assert isinstance(e, collections.Counter)
    assert e == collections.Counter(a=3, b=2)


def test_and(redis: Redis) -> None:
    'Test RedisCounter.__and__()'
    c = RedisCounter(redis=redis, a=3, b=1)
    d = RedisCounter(redis=redis, a=1, b=2)
    e = c & d
    assert isinstance(e, collections.Counter)
    assert e == collections.Counter(a=1, b=1)


def test_pos(redis: Redis) -> None:
    'Test RedisCounter.__pos__()'
    c = RedisCounter(redis=redis, foo=-2, bar=-1, baz=0, qux=1)
    assert isinstance(+c, collections.Counter)
    assert +c == collections.Counter(qux=1)


def test_neg(redis: Redis) -> None:
    'Test RedisCounter.__neg__()'
    c = RedisCounter(redis=redis, foo=-2, bar=-1, baz=0, qux=1)
    assert isinstance(-c, collections.Counter)
    assert -c == collections.Counter(foo=2, bar=1)


def test_in_place_add_with_empty_counter(redis: Redis) -> None:
    'Test RedisCounter.__iadd__() with an empty counter'
    c = RedisCounter(redis=redis, a=1, b=2)
    d = RedisCounter(redis=redis)
    c += d  # type: ignore
    assert isinstance(c, RedisCounter)
    assert c == collections.Counter(a=1, b=2)


def test_in_place_add_with_overlapping_counter(redis: Redis) -> None:
    'Test RedisCounter.__iadd__() with a counter with overlapping keys'
    c = RedisCounter(redis=redis, a=4, b=2, c=0, d=-2)
    d = RedisCounter(redis=redis, a=1, b=2, c=3, d=4)
    c += d  # type: ignore
    assert isinstance(c, RedisCounter)
    assert c == collections.Counter(a=5, b=4, c=3, d=2)


def test_in_place_add_removes_zeroes(redis: Redis) -> None:
    c = RedisCounter(redis=redis, a=4, b=2, c=0, d=-2)
    d: Counter[str] = collections.Counter(a=-4, b=-2, c=0, d=2)
    c += d  # type: ignore
    assert isinstance(c, RedisCounter)
    assert c == collections.Counter()


def test_in_place_subtract(redis: Redis) -> None:
    'Test RedisCounter.__isub__()'
    c = RedisCounter(redis=redis, a=4, b=2, c=0, d=-2)
    d = RedisCounter(redis=redis, a=1, b=2, c=3, d=4)
    c -= d  # type: ignore
    assert isinstance(c, RedisCounter)
    assert c == collections.Counter(a=3)


def test_in_place_or_with_two_empty_counters(redis: Redis) -> None:
    'Test RedisCounter.__ior__() with two empty counters'
    c = RedisCounter(redis=redis)
    d = RedisCounter(redis=redis)
    c |= d  # type: ignore
    assert isinstance(c, RedisCounter)
    assert c == collections.Counter()


def test_in_place_or_with_two_overlapping_counters(redis: Redis) -> None:
    'Test RedisCounter.__ior__() with two counters with overlapping keys'
    c = RedisCounter(redis=redis, a=4, b=2, c=0, d=-2)
    d: Counter[str] = collections.Counter(a=1, b=2, c=3, d=4)
    c |= d  # type: ignore
    assert isinstance(c, RedisCounter)
    assert c == collections.Counter(a=4, b=2, c=3, d=4)


def test_in_place_and(redis: Redis) -> None:
    'Test RedisCounter.__iand__()'
    c = RedisCounter(redis=redis, a=4, b=2, c=0, d=-2)
    d = RedisCounter(redis=redis, a=1, b=2, c=3, d=4)
    c &= d  # type: ignore
    assert isinstance(c, RedisCounter)
    assert c == collections.Counter(a=1, b=2)


def test_in_place_and_results_in_empty_counter(redis: Redis) -> None:
    c = RedisCounter(redis=redis, a=4, b=2)
    d = RedisCounter(redis=redis, c=3, d=4)
    c &= d  # type: ignore
    assert isinstance(c, RedisCounter)
    assert c == collections.Counter()


def test_method_resolution_order() -> None:
    # We need for _Common to come ahead of collections.Counter in the
    # inheritance chain.  Because when we instantiate a RedisCounter, we
    # need to hit _Common's .__init__() first, which *doesn't* delegate to
    # super().__init__(), which *prevents* collections.Counter's
    # .__init__() from getting hit; and this is the correct behavior.
    #
    # RedisCounter inherits from collections.Counter for method reuse; not
    # to initialize a collections.Counter and store key/value pairs in
    # memory.
    #
    # Inspired by Raymond Hettinger's excellent Python's super() considered
    # super!
    #   https://rhettinger.wordpress.com/2011/05/26/super-considered-super/
    position = RedisCounter.mro().index
    assert position(_Common) < position(collections.Counter)
