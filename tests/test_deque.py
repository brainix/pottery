#-----------------------------------------------------------------------------#
#   test_deque.py                                                             #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from pottery import RedisDeque
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
