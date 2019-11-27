# --------------------------------------------------------------------------- #
#   test_bloom.py                                                             #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import math
import random
import string

from pottery import BloomFilter
from tests.base import TestCase


class BloomFilterTests(TestCase):
    _KEY = '{}dilberts'.format(TestCase._TEST_KEY_PREFIX)

    def test_init_without_iterable(self):
        'Test BloomFilter.__init__() without an iterable for initialization'
        dilberts = BloomFilter(num_values=100, false_positives=0.01)
        assert dilberts.num_values == 100
        assert dilberts.false_positives == 0.01
        assert 'rajiv' not in dilberts
        assert 'raj' not in dilberts
        assert 'dan' not in dilberts
        assert 'eric' not in dilberts
        assert dilberts._num_bits_set() == 0
        assert len(dilberts) == 0

    def test_init_with_iterable(self):
        'Test BloomFilter.__init__() with an iterable for initialization'
        dilberts = BloomFilter(
            {'rajiv', 'raj'},
            num_values=100,
            false_positives=0.01,
        )
        assert dilberts.num_values == 100
        assert dilberts.false_positives == 0.01
        assert 'rajiv' in dilberts
        assert 'raj' in dilberts
        assert 'dan' not in dilberts
        assert 'eric' not in dilberts
        # We've inserted two elements into dilberts: 'rajiv' and 'raj'.  So
        # unless dilberts._bit_offsets('rajiv') and
        # dilberts._bit_offsets('raj') perfectly collide/overlap, they differ
        # by at least 1 bit, hence dilberts.num_hashes() + 1:
        assert dilberts._num_bits_set() > dilberts.num_hashes() + 1
        assert len(dilberts) == 2

    def test_size_and_num_hashes(self):
        'Test BloomFilter.size()'
        dilberts = BloomFilter(num_values=100, false_positives=0.1)
        assert dilberts.size() == 480
        assert dilberts.num_hashes() == 4

        dilberts = BloomFilter(num_values=1000, false_positives=0.1)
        assert dilberts.size() == 4793
        assert dilberts.num_hashes() == 4

        dilberts = BloomFilter(num_values=100, false_positives=0.01)
        assert dilberts.size() == 959
        assert dilberts.num_hashes() == 7

        dilberts = BloomFilter(num_values=1000, false_positives=0.01)
        assert dilberts.size() == 9586
        assert dilberts.num_hashes() == 7

    def test_add(self):
        'Test BloomFilter add(), __contains__(), and __len__()'
        dilberts = BloomFilter(num_values=100, false_positives=0.01)
        assert 'rajiv' not in dilberts
        assert 'raj' not in dilberts
        assert 'dan' not in dilberts
        assert 'eric' not in dilberts
        assert len(dilberts) == 0

        dilberts.add('rajiv')
        assert 'rajiv' in dilberts
        assert 'raj' not in dilberts
        assert 'dan' not in dilberts
        assert 'eric' not in dilberts
        assert len(dilberts) == 1

        dilberts.add('raj')
        assert 'rajiv' in dilberts
        assert 'raj' in dilberts
        assert 'dan' not in dilberts
        assert 'eric' not in dilberts
        assert len(dilberts) == 2

        dilberts.add('rajiv')
        assert 'rajiv' in dilberts
        assert 'raj' in dilberts
        assert 'dan' not in dilberts
        assert 'eric' not in dilberts
        assert len(dilberts) == 2

        dilberts.add('raj')
        assert 'rajiv' in dilberts
        assert 'raj' in dilberts
        assert 'dan' not in dilberts
        assert 'eric' not in dilberts
        assert len(dilberts) == 2

        dilberts.add('dan')
        assert 'rajiv' in dilberts
        assert 'raj' in dilberts
        assert 'dan' in dilberts
        assert 'eric' not in dilberts
        assert len(dilberts) == 3

        dilberts.add('eric')
        assert 'rajiv' in dilberts
        assert 'raj' in dilberts
        assert 'dan' in dilberts
        assert 'eric' in dilberts
        assert len(dilberts) == 4

    def test_update(self):
        'Test BloomFilter update(), __contains__(), and __len__()'
        dilberts = BloomFilter(num_values=100, false_positives=0.01)
        assert 'rajiv' not in dilberts
        assert 'raj' not in dilberts
        assert 'dan' not in dilberts
        assert 'eric' not in dilberts
        assert 'jenny' not in dilberts
        assert 'will' not in dilberts
        assert 'rhodes' not in dilberts
        assert len(dilberts) == 0

        dilberts.update({'rajiv', 'raj'}, {'dan', 'eric'})
        assert 'rajiv' in dilberts
        assert 'raj' in dilberts
        assert 'dan' in dilberts
        assert 'eric' in dilberts
        assert 'jenny' not in dilberts
        assert 'will' not in dilberts
        assert 'rhodes' not in dilberts
        assert len(dilberts) == 4

        dilberts.update({'jenny', 'will'})
        assert 'rajiv' in dilberts
        assert 'raj' in dilberts
        assert 'dan' in dilberts
        assert 'eric' in dilberts
        assert 'jenny' in dilberts
        assert 'will' in dilberts
        assert 'rhodes' not in dilberts
        assert len(dilberts) == 6

        dilberts.update(set())
        assert 'rajiv' in dilberts
        assert 'raj' in dilberts
        assert 'dan' in dilberts
        assert 'eric' in dilberts
        assert 'jenny' in dilberts
        assert 'will' in dilberts
        assert 'rhodes' not in dilberts
        assert len(dilberts) == 6

    def test_repr(self):
        'Test BloomFilter.__repr__()'
        dilberts = BloomFilter(
            num_values=100,
            false_positives=0.01,
            key=self._KEY,
        )
        assert repr(dilberts) == '<BloomFilter key={}>'.format(self._KEY)


class RecentlyConsumedTests(TestCase):
    "Simulate reddit's recently consumed problem to test our Bloom filter."

    _KEY = '{}recently-consumed'.format(TestCase._TEST_KEY_PREFIX)

    def setUp(self):
        super().setUp()

        # Construct a set of links that the user has seen.
        self.seen_links = set()
        while len(self.seen_links) < 100:
            fullname = self.random_fullname()
            self.seen_links.add(fullname)

        # Construct a set of links that the user hasn't seen.  Ensure that
        # there's no intersection between the seen set and the unseen set.
        self.unseen_links = set()
        while len(self.unseen_links) < 100:
            fullname = self.random_fullname()
            if fullname not in self.seen_links:
                self.unseen_links.add(fullname)

        # Initialize the recently consumed Bloom filter on the seen set.
        self.recently_consumed = BloomFilter(
            self.seen_links,
            num_values=1000,
            false_positives=0.001,
            key=self._KEY,
        )

    def tearDown(self):
        super().tearDown()

    @staticmethod
    def random_fullname(*, prefix='t3_', size=6):
        alphabet, id36 = string.digits + string.ascii_lowercase, ''
        for _ in range(size):
            id36 += random.choice(alphabet)
        return prefix + id36

    @staticmethod
    def round(number, *, sig_digits=1):
        '''Round a float to the specified number of significant digits.

        Reference implementation:
            https://github.com/ActiveState/code/blob/3b27230f418b714bc9a0f897cb8ea189c3515e99/recipes/Python/578114_Round_number_specified_number_significant/recipe-578114.py
        '''
        try:
            ndigits = sig_digits - 1 - math.floor(math.log10(abs(number)))
        except ValueError:
            # math.log10(number) raised a ValueError, so number must be 0.0.
            # No need to round 0.0.
            return number
        else:
            return round(number, ndigits)

    def test_zero_false_negatives(self):
        'Ensure that we produce zero false negatives'
        for seen_link in self.seen_links:
            assert seen_link in self.recently_consumed

    def test_acceptable_false_positives(self):
        'Ensure that we produce false positives at an acceptable rate'
        acceptable, actual = self.recently_consumed.false_positives, 0

        for unseen_link in self.unseen_links:
            actual += unseen_link in self.recently_consumed
        actual /= len(self.unseen_links)
        actual = self.round(actual, sig_digits=1)

        message = 'acceptable: {}; actual: {}'.format(acceptable, actual)
        assert actual <= acceptable, message
