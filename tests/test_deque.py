# --------------------------------------------------------------------------- #
#   test_deque.py                                                             #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import unittest.mock

from pottery import RedisDeque
from pottery.base import Base
from tests.base import TestCase  # type: ignore


class DequeTests(TestCase):
    '''These tests come from these examples:
        https://docs.python.org/3/library/collections.html#collections.deque
    '''

    def test_basic_usage(self):
        d = RedisDeque('ghi')
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

        assert RedisDeque(reversed(d)) == ['l', 'k', 'j', 'i', 'h', 'g']
        d.clear()
        with self.assertRaises(IndexError):
            d.pop()

        d.extendleft('abc')
        assert d == ['c', 'b', 'a']

    def test_init_with_wrong_type_maxlen(self):
        with unittest.mock.patch.object(Base, '__del__') as delete, \
             self.assertRaises(TypeError):
            delete.return_value = None
            RedisDeque(maxlen='2')

    def test_persistent_deque_bigger_than_maxlen(self):
        d1 = RedisDeque('ghi')
        d1  # Workaround for Pyflakes.  :-(
        with self.assertRaises(IndexError):
            RedisDeque(key=d1.key, maxlen=0)

    def test_maxlen_not_writable(self):
        d = RedisDeque()
        with self.assertRaises(AttributeError):
            d.maxlen = 2

    def test_insert_into_full(self):
        d = RedisDeque('gh', maxlen=3)
        d.insert(len(d), 'i')
        assert d == ['g', 'h', 'i']

        with self.assertRaises(IndexError):
            d.insert(len(d), 'j')

    def test_append_trims_when_full(self):
        d = RedisDeque('gh', maxlen=3)
        d.append('i')
        assert d == ['g', 'h', 'i']
        d.append('j')
        assert d == ['h', 'i', 'j']
        d.appendleft('g')
        assert d == ['g', 'h', 'i']

    def test_extend(self):
        d = RedisDeque('ghi', maxlen=4)
        d.extend('jkl')
        assert d == ['i', 'j', 'k', 'l']
        d.extendleft('hg')
        assert d == ['g', 'h', 'i', 'j']

    def test_popleft_from_empty(self):
        d = RedisDeque()
        with self.assertRaises(IndexError):
            d.popleft()

    def test_rotate_zero_steps(self):
        d = RedisDeque(('g', 'h', 'i', 'j', 'k', 'l'))
        d.rotate(0)
        assert d == ['g', 'h', 'i', 'j', 'k', 'l']

    def test_repr(self):
        d = RedisDeque()
        assert repr(d) == 'RedisDeque([])'

        d = RedisDeque('ghi')
        assert repr(d) == "RedisDeque(['g', 'h', 'i'])"

        d = RedisDeque(maxlen=2)
        assert repr(d) == 'RedisDeque([], maxlen=2)'

        d = RedisDeque('ghi', maxlen=2)
        assert repr(d) == "RedisDeque(['h', 'i'], maxlen=2)"
