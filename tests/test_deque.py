# --------------------------------------------------------------------------- #
#   test_deque.py                                                             #
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


import itertools
import unittest.mock

from pottery import RedisDeque
from pottery.base import Base
from tests.base import TestCase  # type: ignore


class DequeTests(TestCase):
    '''These tests come from these examples:
        https://docs.python.org/3/library/collections.html#collections.deque
    '''

    def test_basic_usage(self):
        d = RedisDeque('ghi', redis=self.redis)
        assert d == ['g', 'h', 'i']

        d.append('j')
        d.appendleft('f')
        assert d == ['f', 'g', 'h', 'i', 'j']

        assert d.pop() == 'j'
        assert d.popleft() == 'f'
        assert d == ['g', 'h', 'i']
        assert d[0] == 'g'
        assert d[-1] == 'i'

        assert list(reversed(d)) == ['i', 'h', 'g']
        assert 'h' in d
        d.extend('jkl')
        assert d == ['g', 'h', 'i', 'j', 'k', 'l']
        d.rotate(1)
        assert d == ['l', 'g', 'h', 'i', 'j', 'k']
        d.rotate(-1)
        assert d == ['g', 'h', 'i', 'j', 'k', 'l']

        assert RedisDeque(reversed(d), redis=self.redis) == ['l', 'k', 'j', 'i', 'h', 'g']
        d.clear()
        with self.assertRaises(IndexError):
            d.pop()

        d.extendleft('abc')
        assert d == ['c', 'b', 'a']

    def test_init_with_wrong_type_maxlen(self):
        with unittest.mock.patch.object(Base, '__del__') as delete, \
             self.assertRaises(TypeError):
            delete.return_value = None
            RedisDeque(redis=self.redis, maxlen='2')

    def test_init_with_maxlen(self):
        d = RedisDeque([1, 2, 3, 4, 5, 6], redis=self.redis, maxlen=3)
        assert d == [4, 5, 6]

        d = RedisDeque([1, 2, 3, 4, 5, 6], redis=self.redis, maxlen=0)
        assert d == []

    def test_persistent_deque_bigger_than_maxlen(self):
        d1 = RedisDeque('ghi', redis=self.redis)
        d1  # Workaround for Pyflakes.  :-(
        with self.assertRaises(IndexError):
            RedisDeque(redis=self.redis, key=d1.key, maxlen=0)

    def test_maxlen_not_writable(self):
        d = RedisDeque(redis=self.redis)
        with self.assertRaises(AttributeError):
            d.maxlen = 2

    def test_insert_into_full(self):
        d = RedisDeque('gh', redis=self.redis, maxlen=3)
        d.insert(len(d), 'i')
        assert d == ['g', 'h', 'i']

        with self.assertRaises(IndexError):
            d.insert(len(d), 'j')

    def test_append_trims_when_full(self):
        d = RedisDeque('gh', redis=self.redis, maxlen=3)
        d.append('i')
        assert d == ['g', 'h', 'i']
        d.append('j')
        assert d == ['h', 'i', 'j']
        d.appendleft('g')
        assert d == ['g', 'h', 'i']

    def test_extend(self):
        d = RedisDeque('ghi', redis=self.redis, maxlen=4)
        d.extend('jkl')
        assert d == ['i', 'j', 'k', 'l']
        d.extendleft('hg')
        assert d == ['g', 'h', 'i', 'j']

    def test_popleft_from_empty(self):
        d = RedisDeque(redis=self.redis)
        with self.assertRaises(IndexError):
            d.popleft()

    def test_invalid_rotating(self):
        d = RedisDeque(('g', 'h', 'i', 'j', 'k', 'l'), redis=self.redis)
        for invalid_steps in {None, 'a', 0.5}:
            with self.subTest(invalid_steps=invalid_steps), \
                 self.assertRaises(TypeError):
                d.rotate(invalid_steps)

    def test_rotate_zero_steps(self):
        'Rotating 0 steps is a no-op'
        d = RedisDeque(('g', 'h', 'i', 'j', 'k', 'l'), redis=self.redis)
        d.rotate(0)
        assert d == ['g', 'h', 'i', 'j', 'k', 'l']

    def test_rotate_empty_deque(self):
        'Rotating an empty RedisDeque is a no-op'
        d = RedisDeque(redis=self.redis)
        d.rotate(2)
        assert d == []

    def test_rotate_right(self):
        'A positive number rotates a RedisDeque right'
        # I got this example from here:
        #   https://pymotw.com/2/collections/deque.html#rotating
        d = RedisDeque(range(10), redis=self.redis)
        d.rotate(2)
        assert d == [8, 9, 0, 1, 2, 3, 4, 5, 6, 7]

    def test_rotate_left(self):
        'A negative number rotates a RedisDeque left'
        # I got this example from here:
        #   https://pymotw.com/2/collections/deque.html#rotating
        d = RedisDeque(range(10), redis=self.redis)
        d.rotate(-2)
        assert d == [2, 3, 4, 5, 6, 7, 8, 9, 0, 1]

    def test_moving_average(self):
        'Test RedisDeque-based moving average'

        # I got this recipe from here:
        #   https://docs.python.org/3.9/library/collections.html#deque-recipes
        def moving_average(iterable, n=3):
            it = iter(iterable)
            d = RedisDeque(itertools.islice(it, n-1), redis=self.redis)
            d.appendleft(0)
            s = sum(d)
            for elem in it:
                s += elem - d.popleft()
                d.append(elem)
                yield s / n

        seq = list(moving_average([40, 30, 50, 46, 39, 44]))
        assert seq == [40.0, 42.0, 45.0, 43.0]

    def test_delete_nth(self):
        'Recipe for deleting the nth element from a RedisDeque'
        d = RedisDeque(('g', 'h', 'i', 'j', 'k', 'l'), redis=self.redis)

        # Delete the 3rd element in the deque, or the 'j'.  I got this recipe
        # from here:
        #   https://docs.python.org/3.9/library/collections.html#deque-recipes
        d.rotate(-3)
        e = d.popleft()
        d.rotate(3)
        assert e == 'j'
        assert d == ['g', 'h', 'i', 'k', 'l']

    def test_truthiness(self):
        d = RedisDeque('ghi', redis=self.redis)
        assert bool(d)
        d.clear()
        assert not bool(d)

    def test_repr(self):
        d = RedisDeque(redis=self.redis)
        assert repr(d) == 'RedisDeque([])'

        d = RedisDeque('ghi', redis=self.redis)
        assert repr(d) == "RedisDeque(['g', 'h', 'i'])"

        d = RedisDeque(redis=self.redis, maxlen=2)
        assert repr(d) == 'RedisDeque([], maxlen=2)'

        d = RedisDeque('ghi', redis=self.redis, maxlen=2)
        assert repr(d) == "RedisDeque(['h', 'i'], maxlen=2)"
