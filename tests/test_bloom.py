# --------------------------------------------------------------------------- #
#   test_bloom.py                                                             #
#                                                                             #
#   Copyright © 2015-2025, Rajiv Bakulesh Shah, original author.              #
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
from typing import Set

import pytest
from redis import Redis

from pottery import BloomFilter
from pottery.bloom import BloomFilterABC
from pottery.bloom import _store_on_self


class TestStoreOnSelf:
    @_store_on_self(attr='_expensive_method_call_count')
    def expensive_method_call_count(self) -> int:
        self._call_count += 1
        return self._call_count

    def test_store_on_self(self) -> None:
        self._call_count = 0
        assert self.expensive_method_call_count() == 1
        assert self.expensive_method_call_count() == 1


class TestBloomFilterABC:
    @staticmethod
    def test_abc_cant_be_instantiated() -> None:
        with pytest.raises(TypeError):
            BloomFilterABC()  # type: ignore


class TestBloomFilter:
    @staticmethod
    def test_init_without_iterable(redis: Redis) -> None:
        'Test BloomFilter.__init__() without an iterable for initialization'
        dilberts = BloomFilter(
            redis=redis,
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

    @staticmethod
    def test_init_with_iterable(redis: Redis) -> None:
        'Test BloomFilter.__init__() with an iterable for initialization'
        dilberts = BloomFilter(
            {'rajiv', 'raj'},
            redis=redis,
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

    @staticmethod
    def test_size_and_num_hashes(redis: Redis) -> None:
        'Test BloomFilter.size()'
        dilberts = BloomFilter(
            redis=redis,
            num_elements=100,
            false_positives=0.1,
        )
        assert dilberts.size() == 480
        assert dilberts.num_hashes() == 4

        dilberts = BloomFilter(
            redis=redis,
            num_elements=1000,
            false_positives=0.1,
        )
        assert dilberts.size() == 4793
        assert dilberts.num_hashes() == 4

        dilberts = BloomFilter(
            redis=redis,
            num_elements=100,
            false_positives=0.01,
        )
        assert dilberts.size() == 959
        assert dilberts.num_hashes() == 7

        dilberts = BloomFilter(
            redis=redis,
            num_elements=1000,
            false_positives=0.01,
        )
        assert dilberts.size() == 9586
        assert dilberts.num_hashes() == 7

    @staticmethod
    def test_add(redis: Redis) -> None:
        'Test BloomFilter add(), __contains__(), and __len__()'
        dilberts = BloomFilter(
            redis=redis,
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

    @staticmethod
    def test_update(redis: Redis) -> None:
        'Test BloomFilter update(), __contains__(), and __len__()'
        dilberts = BloomFilter(
            redis=redis,
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

    @staticmethod
    def test_contains_many_metasyntactic_variables(redis: Redis) -> None:
        metasyntactic_variables = BloomFilter(
            {'foo', 'bar', 'zap', 'a'},
            redis=redis,
            num_elements=4,
            false_positives=0.01,
        )
        contains_many = metasyntactic_variables.contains_many('foo', 'bar', 'baz', 'quz')
        assert tuple(contains_many) == (True, True, False, False)

    @staticmethod
    def test_contains_many_uuids(redis: Redis) -> None:
        NUM_ELEMENTS, FALSE_POSITIVES = 5000, 0.01
        known_uuids, unknown_uuids = [], []
        generate_uuid = lambda: str(uuid.uuid4())  # NoQA: E731
        for _ in range(NUM_ELEMENTS):
            known_uuids.append(generate_uuid())  # type: ignore
            unknown_uuids.append(generate_uuid())  # type: ignore
        uuid_bf = BloomFilter(
            known_uuids,
            redis=redis,
            num_elements=NUM_ELEMENTS,
            false_positives=FALSE_POSITIVES,
        )
        num_known_contained = sum(uuid_bf.contains_many(*known_uuids))
        num_unknown_contained = sum(uuid_bf.contains_many(*unknown_uuids))
        assert num_known_contained == NUM_ELEMENTS
        assert num_unknown_contained <= NUM_ELEMENTS * FALSE_POSITIVES * 2, \
            f'{num_unknown_contained} is not <= {NUM_ELEMENTS * FALSE_POSITIVES * 2}'

    @staticmethod
    def test_membership_for_non_jsonifyable_element(redis: Redis) -> None:
        dilberts = BloomFilter(
            redis=redis,
            num_elements=100,
            false_positives=0.01,
        )
        assert not BaseException in dilberts  # type: ignore

    @staticmethod
    def test_repr(redis: Redis) -> None:
        'Test BloomFilter.__repr__()'
        dilberts = BloomFilter(
            redis=redis,
            key='dilberts',
            num_elements=100,
            false_positives=0.01,
        )
        assert repr(dilberts) == '<BloomFilter key=dilberts>'


class TestRecentlyConsumed:
    "Simulate Reddit's recently consumed problem to test our Bloom filter."

    @pytest.fixture(autouse=True)
    def setup(self, redis: Redis) -> None:
        # Construct a set of links that the user has seen.
        self.seen_links: Set[str] = set()
        while len(self.seen_links) < 100:
            fullname = self.random_fullname()
            self.seen_links.add(fullname)

        # Construct a set of links that the user hasn't seen.  Ensure that
        # there's no intersection between the seen set and the unseen set.
        self.unseen_links: Set[str] = set()
        while len(self.unseen_links) < 100:
            fullname = self.random_fullname()
            if fullname not in self.seen_links:  # pragma: no cover
                self.unseen_links.add(fullname)

        # Initialize the recently consumed Bloom filter on the seen set.
        self.recently_consumed = BloomFilter(
            self.seen_links,
            redis=redis,
            key='recently-consumed',
            num_elements=1000,
            false_positives=0.001,
        )

    @staticmethod
    def random_fullname(*, prefix: str = 't3_', size: int = 6) -> str:
        alphabet, id36 = string.digits + string.ascii_lowercase, ''
        for _ in range(size):
            id36 += random.choice(alphabet)  # nosec
        return prefix + id36

    @staticmethod
    def round(number: float, *, sig_digits: int = 1) -> float:
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
