#-----------------------------------------------------------------------------#
#   test_deque.py                                                             #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import unittest.mock

from pottery import RedisDeque
from pottery.base import Base
from tests.base import TestCase



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

    def test_init_with_wrong_type_maxlen(self):
        with unittest.mock.patch.object(Base, '__del__') as delete, \
             self.assertRaises(TypeError):
            delete.return_value = None
            RedisDeque(maxlen='2')

    def test_maxlen_not_writable(self):
        d = RedisDeque()
        with self.assertRaises(AttributeError):
            d.maxlen = 2

    def test_repr(self):
        d = RedisDeque()
        assert repr(d) == 'RedisDeque([])'

        d = RedisDeque('ghi')
        assert repr(d) == "RedisDeque(['g', 'h', 'i'])"

        d = RedisDeque(maxlen=2)
        assert repr(d) == 'RedisDeque([], maxlen=2)'

        d = RedisDeque('ghi', maxlen=2)
        assert repr(d) == "RedisDeque(['h', 'i'], maxlen=2)"
