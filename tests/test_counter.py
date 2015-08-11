#-----------------------------------------------------------------------------#
#   test_counter.py                                                           #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from pottery import RedisCounter
from tests.base import TestCase



class CounterTests(TestCase):
    '''These tests come from these examples:
        https://docs.python.org/3/library/collections.html#counter-objects
    '''

    def test_basic_usage(self):
        c = RedisCounter()
        for word in ('red', 'blue', 'red', 'green', 'blue', 'blue'):
            c[word] += 1
        assert c['blue'] == 3
        assert c['red'] == 2
        assert c['green'] == 1
        assert set(c.keys()) == {'blue', 'red', 'green'}

    def test_constructor(self):
        c = RedisCounter()
        assert set(c.keys()) == set()

    def test_constructor_with_iterable(self):
        c = RedisCounter('gallahad')
        assert c['a'] == 3
        assert c['l'] == 2
        assert c['g'] == 1
        assert c['h'] == 1
        assert c['d'] == 1
        assert set(c.keys()) == {'a', 'l', 'g', 'h', 'd'}

    def test_constructor_with_mapping(self):
        c = RedisCounter({'red': 4, 'blue': 2})
        assert c['red'] == 4
        assert c['blue'] == 2
        assert set(c.keys()) == {'red', 'blue'}

    def test_constructor_with_kwargs(self):
        c = RedisCounter(cats=4, dogs=8)
        assert c['dogs'] == 8
        assert c['cats'] == 4
        assert set(c.keys()) == {'dogs', 'cats'}

    def test_missing_element_doesnt_raise_keyerror(self):
        c = RedisCounter(('eggs', 'ham'))
        assert set(c.keys()) == {'eggs', 'ham'}
        assert c['bacon'] == 0

    def test_setting_0_count_doesnt_remove_element(self):
        c = RedisCounter(('eggs', 'ham'))
        c['sausage'] = 0
        assert set(c.keys()) == {'eggs', 'ham', 'sausage'}

    def test_del_removes_element(self):
        c = RedisCounter(('eggs', 'ham'))
        c['sausage'] = 0
        assert set(c.keys()) == {'eggs', 'ham', 'sausage'}
        del c['sausage']
        assert set(c.keys()) == {'eggs', 'ham'}

    def test_elements(self):
        c = RedisCounter(a=4, b=2, c=0, d=-2)
        assert sorted(c.elements()) == ['a', 'a', 'a', 'a', 'b', 'b']

    def test_most_common(self):
        c = RedisCounter('abracadabra')
        assert sorted(c.most_common(3)) == [('a', 5), ('b', 2), ('r', 2)]

    def test_subtract(self):
        c = RedisCounter(a=4, b=2, c=0, d=-2)
        d = RedisCounter(a=1, b=2, c=3, d=4)
        c.subtract(d)
        assert c['a'] == 3
        assert c['b'] == 0
        assert c['c'] == -3
        assert c['d'] == -6

    def test_math_operations(self):
        c = RedisCounter(a=3, b=1)
        d = RedisCounter(a=1, b=2)
        e = c + d
        assert set(e.keys()) == {'a', 'b'}
        assert e['a'] == 4
        assert e['b'] == 3
        e = c - d
        assert set(e.keys()) == {'a'}
        assert e['a'] == 2
        e = c & d
        assert set(e.keys()) == {'a', 'b'}
        assert e['a'] == 1
        assert e['b'] == 1
        e = c | d
        assert e['a'] == 3
        assert e['b'] == 2

    def test_unary_addition_and_subtraction(self):
        c = RedisCounter(a=2, b=-4)
        d = RedisCounter() + c
        assert set(d.keys()) == {'a'}
        assert d['a'] == 2
        d = RedisCounter() - c
        assert set(d.keys()) == {'b'}
        assert d['b'] == 4
