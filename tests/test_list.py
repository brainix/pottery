# --------------------------------------------------------------------------- #
#   test_list.py                                                              #
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
    https://docs.python.org/3/tutorial/introduction.html#lists
    https://docs.python.org/3/tutorial/datastructures.html#more-on-lists
'''


import json
from typing import Any

import pytest
from redis import Redis

from pottery import KeyExistsError
from pottery import RedisDeque
from pottery import RedisList


KEY = 'squares'


def test_indexerror(redis: Redis) -> None:
    list_ = RedisList(redis=redis)
    with pytest.raises(IndexError):
        list_[0] = 'raj'  # type: ignore


def test_keyexistserror(redis: Redis) -> None:
    squares = RedisList([1, 4, 9, 16, 25], redis=redis, key=KEY)
    squares     # Workaround for Pyflakes.  :-(
    with pytest.raises(KeyExistsError):
        RedisList([1, 4, 9, 16, 25], redis=redis, key=KEY)


def test_init_empty_list(redis: Redis) -> None:
    squares = RedisList(redis=redis, key=KEY)
    assert squares == []


def test_basic_usage(redis: Redis) -> None:
    squares = RedisList([1, 4, 9, 16, 25], redis=redis)
    assert squares == [1, 4, 9, 16, 25]
    assert squares[0] == 1
    assert squares[-1] == 25
    assert squares[-3:] == [9, 16, 25]
    assert squares[:] == [1, 4, 9, 16, 25]
    assert squares + [36, 49, 64, 81, 100] == \
        [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]


def test_mutability_and_append(redis: Redis) -> None:
    cubes = RedisList([1, 8, 27, 65, 125], redis=redis)
    cubes[3] = 64  # type: ignore
    assert cubes == [1, 8, 27, 64, 125]
    cubes.append(216)
    cubes.append(7**3)
    assert cubes == [1, 8, 27, 64, 125, 216, 343]


def test_slicing(redis: Redis) -> None:
    letters = RedisList(['a', 'b', 'c', 'd', 'e', 'f', 'g'], redis=redis)
    assert letters == ['a', 'b', 'c', 'd', 'e', 'f', 'g']
    assert letters[2:5] == ['c', 'd', 'e']
    assert letters[2:5:2] == ['c', 'e']
    assert letters[2:5:3] == ['c']
    assert letters[2:5:4] == ['c']
    letters[2:5] = ['C', 'D', 'E']  # type: ignore
    assert letters == ['a', 'b', 'C', 'D', 'E', 'f', 'g']
    letters[2:5:2] = [None, None]  # type: ignore
    assert letters == ['a', 'b', None, 'D', None, 'f', 'g']
    letters[2:5] = []  # type: ignore
    assert letters == ['a', 'b', 'f', 'g']
    letters[:] = []  # type: ignore
    assert letters == []


def test_len(redis: Redis) -> None:
    letters = RedisList(['a', 'b', 'c', 'd'], redis=redis)
    assert len(letters) == 4


def test_nesting(redis: Redis) -> None:
    a = ['a', 'b', 'c']
    n = [1, 2, 3]
    x = RedisList([a, n], redis=redis)
    assert x == [['a', 'b', 'c'], [1, 2, 3]]
    assert x[0] == ['a', 'b', 'c']
    assert x[0][1] == 'b'


def test_more_on_lists(redis: Redis) -> None:
    a = RedisList([66.25, 333, 333, 1, 1234.5], redis=redis)
    assert (a.count(333), a.count(66.25), a.count('x')) == (2, 1, 0)
    a.insert(2, -1)
    a.append(333)
    assert a == [66.25, 333, -1, 333, 1, 1234.5, 333]
    assert a.index(333) == 1
    a.remove(333)
    assert a == [66.25, -1, 333, 1, 1234.5, 333]
    a.reverse()
    assert a == [333, 1234.5, 1, 333, -1, 66.25]
    a.sort()
    assert a == [-1, 1, 66.25, 333, 333, 1234.5]
    assert a.pop() == 1234.5
    assert a == [-1, 1, 66.25, 333, 333]


def test_using_list_as_stack(redis: Redis) -> None:
    stack = RedisList([3, 4, 5], redis=redis)
    stack.append(6)
    stack.append(7)
    assert stack == [3, 4, 5, 6, 7]
    assert stack.pop() == 7
    assert stack == [3, 4, 5, 6]
    assert stack.pop() == 6
    assert stack.pop() == 5
    assert stack == [3, 4]


def test_del(redis: Redis) -> None:
    a = RedisList([-1, 1, 66.25, 333, 333, 1234.5], redis=redis)
    del a[0]  # type: ignore
    assert a == [1, 66.25, 333, 333, 1234.5]
    del a[2:4]  # type: ignore
    assert a == [1, 66.25, 1234.5]
    del a[:0]  # type: ignore
    assert a == [1, 66.25, 1234.5]
    del a[:]  # type: ignore
    assert a == []


def test_insert_left(redis: Redis) -> None:
    squares = RedisList([9, 16, 25], redis=redis)
    squares.insert(-1, 4)
    assert squares == [4, 9, 16, 25]
    squares.insert(0, 1)
    assert squares == [1, 4, 9, 16, 25]


def test_insert_middle(redis: Redis) -> None:
    nums = RedisList([0, 0, 0, 0], redis=redis)
    nums.insert(2, 2)
    assert nums == [0, 0, 2, 0, 0]


def test_insert_right(redis: Redis) -> None:
    squares = RedisList([1, 4, 9], redis=redis)
    squares.insert(100, 16)
    squares.insert(100, 25)
    assert squares == [1, 4, 9, 16, 25]


def test_extend(redis: Redis) -> None:
    squares = RedisList([1, 4, 9], redis=redis)
    squares.extend([16, 25])
    assert squares == [1, 4, 9, 16, 25]


def test_sort(redis: Redis) -> None:
    squares = RedisList({1, 4, 9, 16, 25}, redis=redis)
    squares.sort()
    assert squares == [1, 4, 9, 16, 25]

    squares.sort(reverse=True)
    assert squares == [25, 16, 9, 4, 1]

    with pytest.raises(NotImplementedError):
        squares.sort(key=str)  # type: ignore


def test_eq_redisdeque_same_redis_key(redis: Redis) -> None:
    list_ = RedisList([1, 4, 9, 16, 25], redis=redis, key=KEY)
    deque = RedisDeque(redis=redis, key=KEY)
    assert not list_ == deque
    assert list_ != deque


def test_eq_same_object(redis: Redis) -> None:
    squares = RedisList([1, 4, 9, 16, 25], redis=redis, key=KEY)
    assert squares == squares
    assert not squares != squares


def test_eq_same_redis_instance_and_key(redis: Redis) -> None:
    squares1 = RedisList([1, 4, 9, 16, 25], redis=redis, key=KEY)
    squares2 = RedisList(redis=redis, key=KEY)
    assert squares1 == squares2
    assert not squares1 != squares2


def test_eq_same_redis_instance_different_keys(redis: Redis) -> None:
    key1 = 'squares1'
    key2 = 'squares2'
    squares1 = RedisList([1, 4, 9, 16, 25], redis=redis, key=key1)
    squares2 = RedisList([1, 4, 9, 16, 25], redis=redis, key=key2)
    assert squares1 == squares2
    assert not squares1 != squares2


def test_eq_different_lengths(redis: Redis) -> None:
    squares1 = RedisList([1, 4, 9, 16, 25], redis=redis)
    squares2 = [1, 4, 9, 16, 25, 36]
    assert not squares1 == squares2
    assert squares1 != squares2


def test_eq_different_items(redis: Redis) -> None:
    squares1 = RedisList([1, 4, 9, 16, 25], redis=redis)
    squares2 = [4, 9, 16, 25, 36]
    assert not squares1 == squares2
    assert squares1 != squares2


def test_eq_unordered_collection(redis: Redis) -> None:
    squares1 = RedisList([1], redis=redis)
    squares2 = {1}
    assert not squares1 == squares2
    assert squares1 != squares2


def test_eq_immutable_sequence(redis: Redis) -> None:
    squares1 = RedisList([1, 4, 9, 16, 25], redis=redis)
    squares2 = (1, 4, 9, 16, 25)
    assert not squares1 == squares2
    assert squares1 != squares2


def test_eq_typeerror(redis: Redis) -> None:
    squares = RedisList([1, 4, 9, 16, 25], redis=redis)
    assert not squares == None
    assert squares != None


def test_repr(redis: Redis) -> None:
    squares = RedisList([1, 4, 9, 16, 25], redis=redis)
    assert repr(squares) == 'RedisList[1, 4, 9, 16, 25]'


def test_pop_out_of_range(redis: Redis) -> None:
    squares = RedisList([1, 4, 9, 16, 25], redis=redis)
    with pytest.raises(IndexError):
        squares.pop(len(squares))


def test_pop_index(redis: Redis) -> None:
    metasyntactic = RedisList(
        ['foo', 'bar', 'baz', 'qux', 'quux', 'corge', 'grault', 'garply', 'waldo', 'fred', 'plugh', 'xyzzy', 'thud'],
        redis=redis,
    )
    assert metasyntactic.pop(1) == 'bar'


def test_remove_nonexistent(redis: Redis) -> None:
    metasyntactic = RedisList(
        ['foo', 'bar', 'baz', 'qux', 'quux', 'corge', 'grault', 'garply', 'waldo', 'fred', 'plugh', 'xyzzy', 'thud'],
        redis=redis,
    )
    with pytest.raises(ValueError):
        metasyntactic.remove('raj')


def test_json_dumps(redis: Redis) -> None:
    metasyntactic = RedisList(
        ['foo', 'bar', 'baz', 'qux', 'quux', 'corge', 'grault', 'garply', 'waldo', 'fred', 'plugh', 'xyzzy', 'thud'],
        redis=redis,
    )
    assert json.dumps(metasyntactic) == (
        '["foo", "bar", "baz", "qux", "quux", "corge", "grault", "garply", '
        '"waldo", "fred", "plugh", "xyzzy", "thud"]'
    )


@pytest.mark.parametrize('invalid_slice', (None, 'a', 0.5))
def test_invalid_slicing(redis: Redis, invalid_slice: Any) -> None:
    letters = RedisList(['a', 'b', 'c', 'd'], redis=redis)
    with pytest.raises(TypeError):
        letters[invalid_slice]


def test_extended_slicing(redis: Redis) -> None:
    python_list = [1, 2, 3, 4, 5]
    redis_list = RedisList(python_list, redis=redis)
    assert redis_list[len(redis_list)-1:3-1:-1] == python_list[len(python_list)-1:3-1:-1]


def test_slice_notation(redis: Redis) -> None:
    # I got these examples from:
    #     https://railsware.com/blog/python-for-machine-learning-indexing-and-slicing-for-lists-tuples-strings-and-other-sequential-types/#Slice_Notation
    nums = RedisList([10, 20, 30, 40, 50, 60, 70, 80, 90], redis=redis)
    assert nums[2:7] == [30, 40, 50, 60, 70]
    assert nums[0:4] == [10, 20, 30, 40]
    assert nums[:5] == [10, 20, 30, 40, 50]
    assert nums[-3:] == [70, 80, 90]
    assert nums[1:-1] == [20, 30, 40, 50, 60, 70, 80]
    assert nums[-3:8] == [70, 80]
    assert nums[-5:-1] == [50, 60, 70, 80]
    assert nums[:-2] == [10, 20, 30, 40, 50, 60, 70]
    assert nums[::2] == [10, 30, 50, 70, 90]
    assert nums[1::2] == [20, 40, 60, 80]
    assert nums[1:-3:2] == [20, 40, 60]
    assert nums[::-1] == [90, 80, 70, 60, 50, 40, 30, 20, 10]
    assert nums[-2::-1] == [80, 70, 60, 50, 40, 30, 20, 10]
    assert nums[-2:1:-1] == [80, 70, 60, 50, 40, 30]
    assert nums[-2:1:-3] == [80, 50]


def test_invalid_slice_assignment(redis: Redis) -> None:
    nums = RedisList([10, 20, 30, 40, 50, 60, 70, 80, 90], redis=redis)
    with pytest.raises(TypeError):
        nums[:] = 10  # type: ignore
