#-----------------------------------------------------------------------------#
#   test_set.py                                                               #
#                                                                             #
#   Copyright (c) 2015-2016, Rajiv Bakulesh Shah.                             #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from pottery import RedisSet
from tests.base import TestCase



class SetTests(TestCase):
    '''These tests come from these examples:
        https://docs.python.org/3/tutorial/datastructures.html#sets
    '''

    def test_basic_usage(self):
        fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}
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

    def test_isdisjoint(self):
        a = RedisSet('abc')
        b = RedisSet('cde')
        assert not a.isdisjoint(b)
        c = RedisSet('def')
        assert a.isdisjoint(c)

    def test_issubset(self):
        a = RedisSet('abc')
        b = RedisSet('abc')
        # assert a.issubset(b)
        # assert b.issubset(a)
        assert a <= b
        assert b <= a
        assert not a < b
        assert not b < a
        c = RedisSet('abcd')
        # assert a.issubset(c)
        # assert not c.issubset(a)
        assert a <= c
        assert not c <= a
        assert a < c
        assert not c < a
        d = RedisSet('def')
        # assert not a.issubset(d)
        # assert not d.issubset(a)
        assert not a <= d
        assert not d <= a
        assert not a < d
        assert not d < a

    def test_issuperset(self):
        a = RedisSet('abc')
        b = RedisSet('abc')
        # assert a.issuperset(b)
        # assert b.issuperset(a)
        assert a >= b
        assert b >= a
        assert not a > b
        assert not b > a
        c = RedisSet('abcd')
        # assert not a.issuperset(c)
        # assert c.issuperset(a)
        assert not a >= c
        assert c >= a
        assert not a > c
        assert c > a
        d = RedisSet('def')
        # assert not a.issuperset(d)
        # assert not d.issuperset(a)
        assert not a >= d
        assert not d >= a
        assert not a > d
        assert not d > a
