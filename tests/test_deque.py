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
