# --------------------------------------------------------------------------- #
#   test_hyper.py                                                             #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


from pottery import HyperLogLog
from tests.base import TestCase  # type: ignore


class HyperLogLogTests(TestCase):
    _KEY = f'{TestCase._TEST_KEY_PREFIX}hll'

    def test_init_without_iterable(self):
        hll = HyperLogLog(redis=self.redis)
        assert len(hll) == 0

    def test_init_with_iterable(self):
        hll = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        assert len(hll) == 4

    def test_add(self):
        hll = HyperLogLog(redis=self.redis)
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
        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        hll2 = HyperLogLog({'a', 'b', 'c', 'foo'}, redis=self.redis)
        hll1.update(hll2)
        assert len(hll1) == 6

        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        hll1.update({'b', 'c', 'd', 'foo'})
        assert len(hll1) == 7

        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        hll1.update(hll2, {'b', 'c', 'd', 'baz'})
        assert len(hll1) == 8

    def test_union(self):
        hll1 = HyperLogLog({'foo', 'bar', 'zap', 'a'}, redis=self.redis)
        hll2 = HyperLogLog({'a', 'b', 'c', 'foo'}, redis=self.redis)
        assert len(hll1.union(hll2)) == 6
        assert len(hll1.union({'b', 'c', 'd', 'foo'})) == 7
        assert len(hll1.union(hll2, {'b', 'c', 'd', 'baz'})) == 8

    def test_repr(self):
        'Test HyperLogLog.__repr__()'
        hll = HyperLogLog(
            {'foo', 'bar', 'zap', 'a'},
            redis=self.redis,
            key=self._KEY,
        )
        assert repr(hll) == f'<HyperLogLog key={self._KEY} len=4>'
