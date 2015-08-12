#-----------------------------------------------------------------------------#
#   test_set.py                                                               #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from pottery import RedisSet
from tests.base import TestCase



class SetTests(TestCase):
    '''These tests come from these examples:
        https://docs.python.org/3/tutorial/datastructures.html#sets
    '''

    def test_basic_usage(self):
        fruits = ('apple', 'orange', 'apple', 'pear', 'orange', 'banana')
        basket = RedisSet(fruits)
        assert basket == {'orange', 'banana', 'pear', 'apple'}
        assert 'orange' in basket
        assert not 'crabgrass' in basket

    def test_set_operations(self):
        a = RedisSet('abracadabra')
        b = RedisSet('alacazam')
        assert a == {'a', 'r', 'b', 'c', 'd'}
        assert a - b == {'r', 'd', 'b'}
        assert isinstance(a - b, RedisSet)
        assert a | b == {'a', 'c', 'r', 'd', 'b', 'm', 'z', 'l'}
        assert isinstance(a | b, RedisSet)
        assert a & b == {'a', 'c'}
        assert isinstance(a & b, RedisSet)
        assert a ^ b == {'r', 'd', 'b', 'm', 'z', 'l'}
        assert isinstance(a ^ b, RedisSet)
