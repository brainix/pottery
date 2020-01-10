# --------------------------------------------------------------------------- #
#   test_hyper.py                                                             #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


from pottery import HyperLogLog
from tests.base import TestCase


class HyperLogLogTests(TestCase):
    _KEY = '{}hll'.format(TestCase._TEST_KEY_PREFIX)

    def test_init_without_iterable(self):
        hll = HyperLogLog()
        assert len(hll) == 0

    def test_init_with_iterable(self):
        hll = HyperLogLog({'foo', 'bar', 'zap', 'a'})
        assert len(hll) == 4

    def test_add(self):
        hll = HyperLogLog()
        hll.add('foo')
        assert len(hll) == 1

        hll.add('bar')
        assert len(hll) == 2

        hll.add('zap')
        assert len(hll) == 3

        hll.add('a')
        assert len(hll) == 4

        hll.add('a')
        assert len(hll) == 4

        hll.add('b')
        assert len(hll) == 5

        hll.add('c')
        assert len(hll) == 6

        hll.add('foo')
        assert len(hll) == 6

    def test_update(self):
        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'})
        hll2 = HyperLogLog({'a', 'b', 'c', 'foo'})
        hll1.update(hll2)
        assert len(hll1) == 6

        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'})
        hll1.update({'b', 'c', 'd', 'foo'})
        assert len(hll1) == 7

        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'})
        hll1.update(hll2, {'b', 'c', 'd', 'baz'})
        assert len(hll1) == 8

    def test_union(self):
        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'})
        hll2 = HyperLogLog({'a', 'b', 'c', 'foo'})
        assert len(hll1.union(hll2)) == 6
        assert len(hll1.union({'b', 'c', 'd', 'foo'})) == 7
        assert len(hll1.union(hll2, {'b', 'c', 'd', 'baz'})) == 8

    def test_repr(self):
        'Test HyperLogLog.__repr__()'
        hll = HyperLogLog({'foo', 'bar', 'zap', 'a'}, key=self._KEY)
        assert repr(hll) == '<HyperLogLog key={} len=4>'.format(self._KEY)
