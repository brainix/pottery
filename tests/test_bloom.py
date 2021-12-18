# --------------------------------------------------------------------------- #
#   test_bloom.py                                                             #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#                                                                             #
#   Licensed under the Apache License, Version 2.0 (the "License");           #
#   you may not use this file except in compliance with the License.          #
#   You may obtain a copy of the License at:                                  #
#       http://www.apache.org/licenses/LICENSE-2.0                            #
#                                                                             #
#   Unless required by applicable law or agreed to in writing, software       #
#   distributed under the License is distributed on an "AS IS" BASIS,         #
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  #
#   See the License for the specific language governing permissions and       #
#   limitations under the License.                                            #
# --------------------------------------------------------------------------- #


import math
import random
import string
import uuid

from pottery import BloomFilter
from pottery.bloom import _store_on_self
from tests.base import TestCase


class StoreOnSelfTests(TestCase):
    def setUp(self):
        super().setUp()
        self._call_count = 0

    @_store_on_self(attr='_expensive_method_call_count')
    def expensive_method_call_count(self):
        self._call_count += 1
        return self._call_count

    def test_store_on_self(self):
        assert self.expensive_method_call_count() == 1
        assert self.expensive_method_call_count() == 1


class BloomFilterTests(TestCase):
    _KEY = 'dilberts'

    def test_init_without_iterable(self):
        'Test BloomFilter.__init__() without an iterable for initialization'
        dilberts = BloomFilter(
            redis=self.redis,
            num_elements=100,
            false_positives=0.01,
        )
        assert dilberts.num_elements == 100
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
            redis=self.redis,
            num_elements=100,
            false_positives=0.01,
        )
        assert dilberts.num_elements == 100
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
        dilberts = BloomFilter(
            redis=self.redis,
            num_elements=100,
            false_positives=0.1,
        )
        assert dilberts.size() == 480
        assert dilberts.num_hashes() == 4

        dilberts = BloomFilter(
            redis=self.redis,
            num_elements=1000,
            false_positives=0.1,
        )
        assert dilberts.size() == 4793
        assert dilberts.num_hashes() == 4

        dilberts = BloomFilter(
            redis=self.redis,
            num_elements=100,
            false_positives=0.01,
        )
        assert dilberts.size() == 959
        assert dilberts.num_hashes() == 7

        dilberts = BloomFilter(
            redis=self.redis,
            num_elements=1000,
            false_positives=0.01,
        )
        assert dilberts.size() == 9586
        assert dilberts.num_hashes() == 7

    def test_add(self):
        'Test BloomFilter add(), __contains__(), and __len__()'
        dilberts = BloomFilter(
            redis=self.redis,
            num_elements=100,
            false_positives=0.01,
        )
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
        dilberts = BloomFilter(
            redis=self.redis,
            num_elements=100,
            false_positives=0.01,
        )
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

    def test_contains_many_metasyntactic_variables(self):
        metasyntactic_variables = BloomFilter(
            {'foo', 'bar', 'zap', 'a'},
            redis=self.redis,
            num_elements=4,
            false_positives=0.01,
        )
        contains_many = metasyntactic_variables.contains_many('foo', 'bar', 'baz', 'quz')
        assert tuple(contains_many) == (True, True, False, False)

    def test_contains_many_uuids(self):
        NUM_ELEMENTS = 5000
        uuid_list = []
        for _ in range(NUM_ELEMENTS):
            uuid_ = str(uuid.uuid4())
            uuid_list.append(uuid_)
        uuid_hll = BloomFilter(
            uuid_list,
            redis=self.redis,
            num_elements=NUM_ELEMENTS,
            false_positives=0.01,
        )
        num_contained = sum(uuid_hll.contains_many(*uuid_list))
        assert num_contained == NUM_ELEMENTS

    def test_membership_for_non_jsonifyable_element(self):
        dilberts = BloomFilter(
            redis=self.redis,
            key=self._KEY,
            num_elements=100,
            false_positives=0.01,
        )
        assert not BaseException in dilberts

    def test_repr(self):
        'Test BloomFilter.__repr__()'
        dilberts = BloomFilter(
            redis=self.redis,
            key=self._KEY,
            num_elements=100,
            false_positives=0.01,
        )
        assert repr(dilberts) == f'<BloomFilter key={self._KEY}>'


class RecentlyConsumedTests(TestCase):
    "Simulate reddit's recently consumed problem to test our Bloom filter."

    _KEY = 'recently-consumed'

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
            if fullname not in self.seen_links:  # pragma: no cover
                self.unseen_links.add(fullname)

        # Initialize the recently consumed Bloom filter on the seen set.
        self.recently_consumed = BloomFilter(
            self.seen_links,
            redis=self.redis,
            key=self._KEY,
            num_elements=1000,
            false_positives=0.001,
        )

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
        else:  # pragma: no cover
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

        message = f'acceptable: {acceptable}; actual: {actual}'
        assert actual <= acceptable, message
