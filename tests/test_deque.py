# --------------------------------------------------------------------------- #
#   test_deque.py                                                             #
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
    https://docs.python.org/3/library/collections.html#collections.deque
'''


import collections
import itertools
import unittest.mock
from typing import Any
from typing import Generator
from typing import Iterable
from typing import cast

import pytest
from redis import Redis

from pottery import RedisDeque
from pottery import RedisList
from pottery.base import Container


def test_basic_usage(redis: Redis) -> None:
    d = RedisDeque('ghi', redis=redis)
    assert d == collections.deque(['g', 'h', 'i'])

    d.append('j')
    d.appendleft('f')
    assert d == collections.deque(['f', 'g', 'h', 'i', 'j'])

    assert d.pop() == 'j'
    assert d.popleft() == 'f'
    assert d == collections.deque(['g', 'h', 'i'])
    assert d[0] == 'g'
    assert d[-1] == 'i'

    assert list(reversed(d)) == ['i', 'h', 'g']
    assert 'h' in d
    d.extend('jkl')
    assert d == collections.deque(['g', 'h', 'i', 'j', 'k', 'l'])
    d.rotate(1)
    assert d == collections.deque(['l', 'g', 'h', 'i', 'j', 'k'])
    d.rotate(-1)
    assert d == collections.deque(['g', 'h', 'i', 'j', 'k', 'l'])

    assert RedisDeque(reversed(d), redis=redis) == collections.deque(['l', 'k', 'j', 'i', 'h', 'g'])
    d.clear()
    with pytest.raises(IndexError):
        d.pop()

    d.extendleft('abc')
    assert d == collections.deque(['c', 'b', 'a'])


def test_init_with_wrong_type_maxlen(redis: Redis) -> None:
    with unittest.mock.patch.object(Container, '__del__') as delete, \
         pytest.raises(TypeError):
        delete.return_value = None
        RedisDeque(redis=redis, maxlen='2')  # type: ignore


def test_init_with_maxlen(redis: Redis) -> None:
    d = RedisDeque([1, 2, 3, 4, 5, 6], redis=redis, maxlen=3)
    assert d == collections.deque([4, 5, 6])

    d = RedisDeque([1, 2, 3, 4, 5, 6], redis=redis, maxlen=0)
    assert d == collections.deque()


def test_persistent_deque_bigger_than_maxlen(redis: Redis) -> None:
    d1 = RedisDeque('ghi', redis=redis)
    d1  # Workaround for Pyflakes.  :-(
    with pytest.raises(IndexError):
        RedisDeque(redis=redis, key=d1.key, maxlen=0)


def test_maxlen_not_writable(redis: Redis) -> None:
    d = RedisDeque(redis=redis)
    with pytest.raises(AttributeError):
        d.maxlen = 2


def test_insert_into_full(redis: Redis) -> None:
    d = RedisDeque('gh', redis=redis, maxlen=3)
    d.insert(len(d), 'i')
    assert d == collections.deque(['g', 'h', 'i'])

    with pytest.raises(IndexError):
        d.insert(len(d), 'j')


def test_append_trims_when_full(redis: Redis) -> None:
    d = RedisDeque('gh', redis=redis, maxlen=3)
    d.append('i')
    assert d == collections.deque(['g', 'h', 'i'])
    d.append('j')
    assert d == collections.deque(['h', 'i', 'j'])
    d.appendleft('g')
    assert d == collections.deque(['g', 'h', 'i'])


def test_extend(redis: Redis) -> None:
    d = RedisDeque('ghi', redis=redis, maxlen=4)
    d.extend('jkl')
    assert d == collections.deque(['i', 'j', 'k', 'l'])
    d.extendleft('hg')
    assert d == collections.deque(['g', 'h', 'i', 'j'])


def test_popleft_from_empty(redis: Redis) -> None:
    d = RedisDeque(redis=redis)
    with pytest.raises(IndexError):
        d.popleft()


@pytest.mark.parametrize('invalid_steps', (None, 'a', 0.5))
def test_invalid_rotating(redis: Redis, invalid_steps: Any) -> None:
    d = RedisDeque(('g', 'h', 'i', 'j', 'k', 'l'), redis=redis)
    for invalid_steps in {None, 'a', 0.5}:
        with pytest.raises(TypeError):
            d.rotate(invalid_steps)


def test_rotate_zero_steps(redis: Redis) -> None:
    'Rotating 0 steps is a no-op'
    d = RedisDeque(('g', 'h', 'i', 'j', 'k', 'l'), redis=redis)
    d.rotate(0)
    assert d == collections.deque(['g', 'h', 'i', 'j', 'k', 'l'])


def test_rotate_empty_deque(redis: Redis) -> None:
    'Rotating an empty RedisDeque is a no-op'
    d = RedisDeque(redis=redis)
    d.rotate(2)
    assert d == collections.deque()


def test_rotate_right(redis: Redis) -> None:
    'A positive number rotates a RedisDeque right'
    # I got this example from here:
    #     https://pymotw.com/2/collections/deque.html#rotating
    d = RedisDeque(range(10), redis=redis)
    d.rotate(2)
    assert d == collections.deque([8, 9, 0, 1, 2, 3, 4, 5, 6, 7])


def test_rotate_left(redis: Redis) -> None:
    'A negative number rotates a RedisDeque left'
    # I got this example from here:
    #     https://pymotw.com/2/collections/deque.html#rotating
    d = RedisDeque(range(10), redis=redis)
    d.rotate(-2)
    assert d == collections.deque([2, 3, 4, 5, 6, 7, 8, 9, 0, 1])


def test_moving_average(redis: Redis) -> None:
    'Test RedisDeque-based moving average'

    # I got this recipe from here:
    #   https://docs.python.org/3.9/library/collections.html#deque-recipes
    def moving_average(iterable: Iterable[int], n: int = 3) -> Generator[float, None, None]:
        it = iter(iterable)
        d = RedisDeque(itertools.islice(it, n-1), redis=redis)
        d.appendleft(0)
        s = sum(d)
        for elem in it:
            s += elem - cast(int, d.popleft())
            d.append(elem)
            yield s / n

    seq = list(moving_average([40, 30, 50, 46, 39, 44]))
    assert seq == [40.0, 42.0, 45.0, 43.0]


def test_delete_nth(redis: Redis) -> None:
    'Recipe for deleting the nth element from a RedisDeque'
    d = RedisDeque(('g', 'h', 'i', 'j', 'k', 'l'), redis=redis)

    # Delete the 3rd element in the deque, or the 'j'.  I got this recipe
    # from here:
    #   https://docs.python.org/3.9/library/collections.html#deque-recipes
    d.rotate(-3)
    e = d.popleft()
    d.rotate(3)
    assert e == 'j'
    assert d == collections.deque(['g', 'h', 'i', 'k', 'l'])


def test_truthiness(redis: Redis) -> None:
    d = RedisDeque('ghi', redis=redis)
    assert bool(d)
    d.clear()
    assert not bool(d)


def test_repr(redis: Redis) -> None:
    d = RedisDeque(redis=redis)
    assert repr(d) == 'RedisDeque([])'

    d = RedisDeque('ghi', redis=redis)
    assert repr(d) == "RedisDeque(['g', 'h', 'i'])"

    d = RedisDeque(redis=redis, maxlen=2)
    assert repr(d) == 'RedisDeque([], maxlen=2)'

    d = RedisDeque('ghi', redis=redis, maxlen=2)
    assert repr(d) == "RedisDeque(['h', 'i'], maxlen=2)"


def test_eq_redislist_same_redis_key(redis: Redis) -> None:
    deque = RedisDeque('ghi', redis=redis)
    list_ = RedisList(redis=redis, key=deque.key)
    assert not deque == list_
    assert deque != list_
