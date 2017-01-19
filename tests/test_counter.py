#-----------------------------------------------------------------------------#
#   test_counter.py                                                           #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import collections

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
        assert c == collections.Counter(blue=3, red=2, green=1)

    def test_constructor(self):
        c = RedisCounter()
        assert c == collections.Counter()

    def test_constructor_with_iterable(self):
        c = RedisCounter('gallahad')
        assert c == collections.Counter(a=3, l=2, g=1, h=1, d=1)

    def test_constructor_with_mapping(self):
        c = RedisCounter({'red': 4, 'blue': 2})
        assert c == collections.Counter(red=4, blue=2)

    def test_constructor_with_kwargs(self):
        c = RedisCounter(cats=4, dogs=8)
        assert c == collections.Counter(dogs=8, cats=4)

    def test_missing_element_doesnt_raise_keyerror(self):
        c = RedisCounter(('eggs', 'ham'))
        assert c['bacon'] == 0

    def test_setting_0_adds_item(self):
        c = RedisCounter(('eggs', 'ham'))
        assert set(c) == {'eggs', 'ham'}
        c['sausage'] = 0
        assert set(c) == {'eggs', 'ham', 'sausage'}

    def test_setting_0_count_doesnt_remove_item(self):
        c = RedisCounter(('eggs', 'ham'))
        c['ham'] = 0
        assert set(c) == {'eggs', 'ham'}

    def test_del_removes_item(self):
        c = RedisCounter(('eggs', 'ham'))
        c['sausage'] = 0
        assert set(c) == {'eggs', 'ham', 'sausage'}
        del c['sausage']
        assert set(c) == {'eggs', 'ham'}
        del c['ham']
        assert set(c) == {'eggs'}

    def test_elements(self):
        c = RedisCounter(a=4, b=2, c=0, d=-2)
        assert sorted(c.elements()) == ['a', 'a', 'a', 'a', 'b', 'b']

    def test_most_common(self):
        c = RedisCounter('abracadabra')
        assert sorted(c.most_common(3)) == [('a', 5), ('b', 2), ('r', 2)]

    def test_update_with_empty_dict(self):
        c = RedisCounter(foo=1)
        c.update({})
        assert isinstance(c, RedisCounter)
        assert c == collections.Counter(foo=1)

    def test_update_with_overlapping_dict(self):
        c = RedisCounter(foo=1, bar=1)
        c.update({'bar': 1, 'baz': 3, 'qux': 4})
        assert isinstance(c, RedisCounter)
        assert c == collections.Counter(foo=1, bar=2, baz=3, qux=4)

    def test_subtract(self):
        c = RedisCounter(a=4, b=2, c=0, d=-2)
        d = RedisCounter(a=1, b=2, c=3, d=4)
        c.subtract(d)
        assert isinstance(c, RedisCounter)
        assert c == collections.Counter(a=3, b=0, c=-3, d=-6)

    def test_repr(self):
        'Test RedisCounter.__repr__().'
        c = RedisCounter(('eggs', 'ham'))
        assert repr(c) in {
            "RedisCounter{'eggs': 1, 'ham': 1}",
            "RedisCounter{'ham': 1, 'eggs': 1}",
        }

    def test_add(self):
        'Test RedisCounter.__add__().'
        c = RedisCounter(a=3, b=1)
        d = RedisCounter(a=1, b=2)
        e = c + d
        assert isinstance(e, collections.Counter)
        assert e == collections.Counter(a=4, b=3)

    def test_sub(self):
        'Test RedisCounter.__sub__().'
        c = RedisCounter(a=3, b=1)
        d = RedisCounter(a=1, b=2)
        e = c - d
        assert isinstance(e, collections.Counter)
        assert e == collections.Counter(a=2)

    def test_or(self):
        'Test RedisCounter.__or__().'
        c = RedisCounter(a=3, b=1)
        d = RedisCounter(a=1, b=2)
        e = c | d
        assert isinstance(e, collections.Counter)
        assert e == collections.Counter(a=3, b=2)

    def test_and(self):
        'Test RedisCounter.__and__().'
        c = RedisCounter(a=3, b=1)
        d = RedisCounter(a=1, b=2)
        e = c & d
        assert isinstance(e, collections.Counter)
        assert e == collections.Counter(a=1, b=1)

    def test_pos(self):
        'Test RedisCounter.__pos__().'
        c = RedisCounter(foo=-2, bar=-1, baz=0, qux=1)
        assert isinstance(+c, collections.Counter)
        assert +c == collections.Counter(qux=1)

    def test_neg(self):
        'Test RedisCounter.__neg__().'
        c = RedisCounter(foo=-2, bar=-1, baz=0, qux=1)
        assert isinstance(-c, collections.Counter)
        assert -c == collections.Counter(foo=2, bar=1)

    def test_in_place_add_with_empty_counter(self):
        'Test RedisCounter.__iadd__() with an empty counter.'
        c = RedisCounter(a=1, b=2)
        d = RedisCounter()
        c += d
        assert isinstance(c, RedisCounter)
        assert c == collections.Counter(a=1, b=2)

    def test_in_place_add_with_overlapping_counter(self):
        'Test RedisCounter.__iadd__() with a counter with overlapping keys.'
        c = RedisCounter(a=4, b=2, c=0, d=-2)
        d = RedisCounter(a=1, b=2, c=3, d=4)
        c += d
        assert isinstance(c, RedisCounter)
        assert c == collections.Counter(a=5, b=4, c=3, d=2)

    def test_in_place_add_removes_zeroes(self):
        c = RedisCounter(a=4, b=2, c=0, d=-2)
        d = collections.Counter(a=-4, b=-2, c=0, d=2)
        c += d
        assert isinstance(c, RedisCounter)
        assert c == collections.Counter()

    def test_in_place_subtract(self):
        'Test RedisCounter.__isub__().'
        c = RedisCounter(a=4, b=2, c=0, d=-2)
        d = RedisCounter(a=1, b=2, c=3, d=4)
        c -= d
        assert isinstance(c, RedisCounter)
        assert c == collections.Counter(a=3)

    def test_in_place_or_with_two_empty_counters(self):
        'Test RedisCounter.__ior__() with two empty counters.'
        c = RedisCounter()
        d = RedisCounter()
        c |= d
        assert isinstance(c, RedisCounter)
        assert c == collections.Counter()

    def test_in_place_or_with_two_overlapping_counters(self):
        'Test RedisCounter.__ior__() with two counters with overlapping keys.'
        c = RedisCounter(a=4, b=2, c=0, d=-2)
        d = collections.Counter(a=1, b=2, c=3, d=4)
        c |= d
        assert isinstance(c, RedisCounter)
        assert c == collections.Counter(a=4, b=2, c=3, d=4)

    def test_in_place_and(self):
        'Test RedisCounter.__iand__().'
        c = RedisCounter(a=4, b=2, c=0, d=-2)
        d = RedisCounter(a=1, b=2, c=3, d=4)
        c &= d
        assert isinstance(c, RedisCounter)
        assert c == collections.Counter(a=1, b=2)

    def test_in_place_and_results_in_empty_counter(self):
        c = RedisCounter(a=4, b=2)
        d = RedisCounter(c=3, d=4)
        c &= d
        assert isinstance(c, RedisCounter)
        assert c == collections.Counter()
