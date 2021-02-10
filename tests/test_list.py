# --------------------------------------------------------------------------- #
#   test_list.py                                                              #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import json

from pottery import KeyExistsError
from pottery import RedisList
from tests.base import TestCase  # type: ignore


class ListTests(TestCase):
    '''These tests come from these examples:
        https://docs.python.org/3/tutorial/introduction.html#lists
        https://docs.python.org/3/tutorial/datastructures.html#more-on-lists
    '''

    _KEY = 'squares'

    def test_indexerror(self):
        list_ = RedisList(redis=self.redis)
        with self.assertRaises(IndexError):
            list_[0] = 'raj'

    def test_keyexistserror(self):
        squares = RedisList((1, 4, 9, 16, 25), redis=self.redis, key=self._KEY)
        squares     # Workaround for Pyflakes.  :-(
        with self.assertRaises(KeyExistsError):
            RedisList((1, 4, 9, 16, 25), redis=self.redis, key=self._KEY)

    def test_init_empty_list(self):
        squares = RedisList(redis=self.redis, key=self._KEY)
        assert squares == []

    def test_basic_usage(self):
        squares = RedisList((1, 4, 9, 16, 25), redis=self.redis)
        assert squares == [1, 4, 9, 16, 25]
        assert squares[0] == 1
        assert squares[-1] == 25
        assert squares[-3:] == [9, 16, 25]
        assert squares[:] == [1, 4, 9, 16, 25]
        assert squares + [36, 49, 64, 81, 100] == \
            [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]

    def test_mutability_and_append(self):
        cubes = RedisList((1, 8, 27, 65, 125), redis=self.redis)
        cubes[3] = 64
        assert cubes == [1, 8, 27, 64, 125]
        cubes.append(216)
        cubes.append(7**3)
        assert cubes == [1, 8, 27, 64, 125, 216, 343]

    def test_slicing(self):
        letters = RedisList(
            ('a', 'b', 'c', 'd', 'e', 'f', 'g'),
            redis=self.redis,
        )
        assert letters == ['a', 'b', 'c', 'd', 'e', 'f', 'g']
        assert letters[2:5] == ['c', 'd', 'e']
        assert letters[2:5:2] == ['c', 'e']
        assert letters[2:5:3] == ['c']
        assert letters[2:5:4] == ['c']
        letters[2:5] = ['C', 'D', 'E']
        assert letters == ['a', 'b', 'C', 'D', 'E', 'f', 'g']
        letters[2:5:2] = [None, None]
        assert letters == ['a', 'b', None, 'D', None, 'f', 'g']
        letters[2:5] = []
        assert letters == ['a', 'b', 'f', 'g']
        letters[:] = []
        assert letters == []

    def test_len(self):
        letters = RedisList(('a', 'b', 'c', 'd'), redis=self.redis)
        assert len(letters) == 4

    def test_nesting(self):
        a = ['a', 'b', 'c']
        n = [1, 2, 3]
        x = RedisList((a, n), redis=self.redis)
        assert x == [['a', 'b', 'c'], [1, 2, 3]]
        assert x[0] == ['a', 'b', 'c']
        assert x[0][1] == 'b'

    def test_more_on_lists(self):
        a = RedisList((66.25, 333, 333, 1, 1234.5), redis=self.redis)
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

    def test_using_list_as_stack(self):
        stack = RedisList((3, 4, 5), redis=self.redis)
        stack.append(6)
        stack.append(7)
        assert stack == [3, 4, 5, 6, 7]
        assert stack.pop() == 7
        assert stack == [3, 4, 5, 6]
        assert stack.pop() == 6
        assert stack.pop() == 5
        assert stack == [3, 4]

    def test_del(self):
        a = RedisList((-1, 1, 66.25, 333, 333, 1234.5), redis=self.redis)
        del a[0]
        assert a == [1, 66.25, 333, 333, 1234.5]
        del a[2:4]
        assert a == [1, 66.25, 1234.5]
        del a[:0]
        assert a == [1, 66.25, 1234.5]
        del a[:]
        assert a == []

    def test_insert_left(self):
        squares = RedisList((9, 16, 25), redis=self.redis)
        squares.insert(-1, 4)
        assert squares == [4, 9, 16, 25]
        squares.insert(0, 1)
        assert squares == [1, 4, 9, 16, 25]

    def test_extend(self):
        squares = RedisList((1, 4, 9), redis=self.redis)
        squares.extend((16, 25))
        assert squares == [1, 4, 9, 16, 25]

    def test_sort(self):
        squares = RedisList({1, 4, 9, 16, 25}, redis=self.redis)
        squares.sort()
        assert squares == [1, 4, 9, 16, 25]

        squares.sort(reverse=True)
        assert squares == [25, 16, 9, 4, 1]

        with self.assertRaises(NotImplementedError):
            squares.sort(key=str)

    def test_eq_same_redis_instance_and_key(self):
        squares1 = RedisList((1, 4, 9, 16, 25), redis=self.redis, key=self._KEY)
        squares2 = RedisList(redis=self.redis, key=self._KEY)
        assert squares1 == squares2
        assert not squares1 != squares2

    def test_eq_same_redis_instance_different_keys(self):
        key1 = 'squares1'
        key2 = 'squares2'
        squares1 = RedisList((1, 4, 9, 16, 25), redis=self.redis, key=key1)
        squares2 = RedisList((1, 4, 9, 16, 25), redis=self.redis, key=key2)
        assert squares1 == squares2
        assert not squares1 != squares2

    def test_eq_different_lengths(self):
        squares1 = RedisList((1, 4, 9, 16, 25), redis=self.redis)
        squares2 = (1, 4, 9, 16, 25, 36)
        assert not squares1 == squares2
        assert squares1 != squares2

    def test_eq_different_items(self):
        squares1 = RedisList((1, 4, 9, 16, 25), redis=self.redis)
        squares2 = (4, 9, 16, 25, 36)
        assert not squares1 == squares2
        assert squares1 != squares2

    def test_eq_unordered_collection(self):
        squares1 = RedisList((1,), redis=self.redis)
        squares2 = {1}
        assert not squares1 == squares2
        assert squares1 != squares2

    def test_eq_typeerror(self):
        squares = RedisList((1, 4, 9, 16, 25), redis=self.redis)
        assert not squares == None
        assert squares != None

    def test_repr(self):
        squares = RedisList((1, 4, 9, 16, 25), redis=self.redis)
        assert repr(squares) == 'RedisList[1, 4, 9, 16, 25]'

    def test_pop_out_of_range(self):
        squares = RedisList((1, 4, 9, 16, 25), redis=self.redis)
        with self.assertRaises(IndexError):
            squares.pop(len(squares))

    def test_pop_index(self):
        metasyntactic = RedisList(
            ('foo', 'bar', 'baz', 'qux', 'quux', 'corge', 'grault', 'garply', 'waldo', 'fred', 'plugh', 'xyzzy', 'thud'),
            redis=self.redis,
        )
        assert metasyntactic.pop(1) == 'bar'

    def test_remove_nonexistent(self):
        metasyntactic = RedisList(
            ('foo', 'bar', 'baz', 'qux', 'quux', 'corge', 'grault', 'garply', 'waldo', 'fred', 'plugh', 'xyzzy', 'thud'),
            redis=self.redis,
        )
        with self.assertRaises(ValueError):
            metasyntactic.remove('raj')

    def test_json_dumps(self):
        metasyntactic = RedisList(
            ('foo', 'bar', 'baz', 'qux', 'quux', 'corge', 'grault', 'garply', 'waldo', 'fred', 'plugh', 'xyzzy', 'thud'),
            redis=self.redis,
        )
        assert json.dumps(metasyntactic) == (
            '["foo", "bar", "baz", "qux", "quux", "corge", "grault", "garply", '
            '"waldo", "fred", "plugh", "xyzzy", "thud"]'
        )

    def test_invalid_slicing(self):
        letters = RedisList(('a', 'b', 'c', 'd'), redis=self.redis)
        for invalid_slice in {'a', 0.5}:
            with self.subTest(invalid_slice=invalid_slice), \
                 self.assertRaises(TypeError):
                letters[invalid_slice]

    def test_extended_slicing(self):
        python_list = [1, 2, 3, 4, 5]
        redis_list = RedisList(python_list, redis=self.redis)
        assert redis_list[len(redis_list)-1:3-1:-1] == python_list[len(python_list)-1:3-1:-1]

    def test_slice_notation(self):
        # I got these examples from:
        #   https://railsware.com/blog/python-for-machine-learning-indexing-and-slicing-for-lists-tuples-strings-and-other-sequential-types/#Slice_Notation
        nums = RedisList((10, 20, 30, 40, 50, 60, 70, 80, 90), redis=self.redis)
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
