# --------------------------------------------------------------------------- #
#   bloom.py                                                                  #
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


import abc
import functools
import itertools
import math
import uuid
from typing import Any
from typing import Callable
from typing import Generator
from typing import Iterable
from typing import Set
from typing import cast

import mmh3

from .annotations import F
from .base import Base
from .base import JSONTypes


def _store_on_self(*, attr: str) -> Callable[[F], F]:
    "Decorator to store/cache a method's return value as an attribute on self."
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                value = getattr(self, attr)
            except AttributeError:
                value = func(self, *args, **kwargs)
                setattr(self, attr, value)
            return value
        return cast(F, wrapper)
    return decorator


class BloomFilterABC(metaclass=abc.ABCMeta):
    '''Bloom filter abstract base class.

    This abstract base class:
        1. Defines the abstract methods that the concrete implementation class
           must define (having to do with encoding/decoding, I/O, and storage)
        2. Implements numerical recipes on the number of elements that you want
           to insert, and the acceptable false positive rate
    '''

    @abc.abstractmethod
    def _bit_offsets(self,
                     encoded_value: JSONTypes,
                     ) -> Generator[int, None, None]:
        for seed in range(self.num_hashes()):
            hash_ = mmh3.hash(encoded_value, seed=seed)
            yield hash_ % self.size()

    @abc.abstractmethod
    def update(self, *iterables: Iterable[JSONTypes]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def __contains__(self, value: JSONTypes) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def _num_bits_set(self) -> int:
        raise NotImplementedError

    def __init__(self,
                 iterable: Iterable[JSONTypes] = frozenset(),
                 *args: Any,
                 num_elements: int,
                 false_positives: float,
                 **kwargs: Any,
                 ) -> None:
        '''Initialize the BloomFilter.  O(n * k)

        Here, n is the number of elements in iterable that you want to insert
        into this Bloom filter, and k is the number of times to run our hash
        functions on each element.
        '''
        super().__init__(*args, **kwargs)  # type: ignore
        self.num_elements = num_elements
        self.false_positives = false_positives
        self.update(iterable)

    @_store_on_self(attr='_size')
    def size(self) -> int:
        '''The required number of bits (m) given n and p.

        This method returns the required number of bits (m) for the underlying
        string representing this Bloom filter given the the number of elements
        that you expect to insert (n) and your acceptable false positive
        probability (p).

        More about the formula that this method implements:
            https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
        '''
        size = (
            -self.num_elements
            * math.log(self.false_positives)
            / math.log(2)**2
        )
        size = math.ceil(size)
        return size

    @_store_on_self(attr='_num_hashes')
    def num_hashes(self) -> int:
        '''The number of hash functions (k) given m and n, minimizing p.

        This method returns the number of times (k) to run our hash functions
        on a given input string to compute bit offsets into the underlying
        string representing this Bloom filter.  m is the size in bits of the
        underlying string, n is the number of elements that you expect to
        insert, and p is your acceptable false positive probability.

        More about the formula that this method implements:
            https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
        '''
        num_hashes = self.size() / self.num_elements * math.log(2)
        num_hashes = math.ceil(num_hashes)
        return num_hashes

    def add(self, value: JSONTypes) -> None:
        '''Add an element to the BloomFilter.  O(k)

        Here, k is the number of times to run our hash functions on a given
        input string to compute bit offests into the underlying string
        representing this Bloom filter.
        '''
        self.update({value})

    def __len__(self) -> int:
        '''Return the approximate the number of elements in the BloomFilter.  O(m)

        Here, m is the size in bits of the underlying string representing this
        Bloom filter.

        Please note that this method returns an approximation, not an exact
        value, though it's quite accurate.

        More about the formula that this method implements:
            https://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter
        '''
        len_ = (
            -self.size()
            / self.num_hashes()
            * math.log(1 - self._num_bits_set() / self.size())
        )
        return math.floor(len_)


class BloomFilter(BloomFilterABC, Base):
    '''Redis-backed Bloom filter with an API similar to Python sets.

    Bloom filters are a powerful data structure that help you to answer the
    question, "Have I seen this element before?" but not the question, "What
    are all of the elements that I've seen before?"  So think of Bloom filters
    as Python sets that you can add elements to and use to test element
    membership, but that you can't iterate through or get elements back out of.

    Bloom filters are probabilistic, which means that they can sometimes
    generate false positives (as in, they may report that you've seen a
    particular element before even though you haven't).  But they will never
    generate false negatives (so every time that they report that you haven't
    seen a particular element before, you really must never have seen it).  You
    can tune your acceptable false positive probability, though at the expense
    of the storage size and the element insertion/lookup time of your Bloom
    filter.

    Wikipedia article:
        https://en.wikipedia.org/wiki/Bloom_filter

    Reference implementation:
        http://www.maxburstein.com/blog/creating-a-simple-bloom-filter/

    Instantiate a Bloom filter and clean up Redis before the doctest:

        >>> dilberts = BloomFilter(
        ...     num_elements=100,
        ...     false_positives=0.01,
        ...     key='dilberts',
        ... )
        >>> dilberts.clear()

    Here, num_elements represents the number of elements that you expect to
    insert into your BloomFilter, and false_positives represents your
    acceptable false positive probability.  Using these two parameters,
    BloomFilter automatically computes its own storage size and number of times
    to run its hash functions on element insertion/lookup such that it can
    guarantee a false positive rate at or below what you can tolerate, given
    that you're going to insert your specified number of elements.

    Insert an element into the Bloom filter:

        >>> dilberts.add('rajiv')

    Test for membership in the Bloom filter:

        >>> 'rajiv' in dilberts
        True
        >>> 'raj' in dilberts
        False
        >>> 'dan' in dilberts
        False

    See how many elements we've inserted into the Bloom filter:

        >>> len(dilberts)
        1

    Note that BloomFilter.__len__() is an approximation, not an exact value,
    though it's quite accurate.

    Insert multiple elements into the Bloom filter:

        >>> dilberts.update({'raj', 'dan'})

    Remove all of the elements from the Bloom filter:

        >>> dilberts.clear()
    '''

    def _bit_offsets(self, value: JSONTypes) -> Generator[int, None, None]:
        '''The bit offsets to set/check in this Bloom filter for a given value.

        Instantiate a Bloom filter:

            >>> dilberts = BloomFilter(
            ...     num_elements=100,
            ...     false_positives=0.01,
            ...     key='dilberts',
            ... )

        Now let's look at a few examples:

            >>> tuple(dilberts._bit_offsets('rajiv'))
            (183, 319, 787, 585, 8, 471, 711)
            >>> tuple(dilberts._bit_offsets('raj'))
            (482, 875, 725, 667, 109, 714, 595)
            >>> tuple(dilberts._bit_offsets('dan'))
            (687, 925, 954, 707, 615, 914, 620)

        Thus, if we want to insert the value 'rajiv' into our Bloom filter,
        then we must set bits 183, 319, 787, 585, 8, 471, and 711 all to 1.  If
        any/all of them are already 1, no problems.

        Similarly, if we want to check to see if the value 'rajiv' is in our
        Bloom filter, then we must check to see if the bits 183, 319, 787, 585,
        8, 471, and 711 are all set to 1.  If even one of those bits is set to
        0, then the value 'rajiv' must never have been inserted into our Bloom
        filter.  But if all of those bits are set to 1, then the value 'rajiv'
        has *probably* been inserted into our Bloom filter.
        '''
        encoded_value = self._encode(value)
        return super()._bit_offsets(encoded_value)

    def update(self, *iterables: Iterable[JSONTypes]) -> None:
        '''Populate the Bloom filter with the elements in iterables.  O(n * k)

        Here, n is the number of elements in iterables that you want to insert
        into this Bloom filter, and k is the number of times to run our hash
        functions on each element.
        '''
        with self._watch(*iterables) as pipeline:
            bit_offsets: Set[int] = set()
            for value in itertools.chain(*iterables):
                bit_offsets.update(self._bit_offsets(value))

            pipeline.multi()
            for bit_offset in bit_offsets:
                pipeline.setbit(self.key, bit_offset, 1)

    def __contains__(self, value: JSONTypes) -> bool:
        '''bf.__contains__(element) <==> element in bf.  O(k)

        Here, k is the number of times to run our hash functions on a given
        input string to compute bit offests into the underlying string
        representing this Bloom filter.

        Please note that this method *may* return false positives, but *never*
        returns false negatives.  This means that if `element in bf` evaluates
        to True, then you *may* have inserted the element into the Bloom filter.
        But if `element in bf` evaluates to False, then you *must not* have
        inserted it.
        '''
        return next(self.__contains_many(value))

    def contains_many(self, *values: JSONTypes) -> Generator[bool, None, None]:
        '''Yield whether this Bloom filter contains multiple elements.  O(n)

        Please note that this method *may* return false positives, but *never*
        returns false negatives.  This means that if .contains_many() yields
        True, then you *may* have inserted the element into the Bloom filter.
        But if .contains_many() yields False, then you *must not* have inserted
        it.
        '''
        with self._watch() as pipeline:
            pipeline.multi()
            for bit_offset in self.__bit_offsets_many(*values):
                pipeline.getbit(self.key, bit_offset)
            bits = iter(pipeline.execute())

        # I stole this recipe from here:
        #   https://stackoverflow.com/a/61435714
        #
        # TODO: When we drop support for Python 3.7, rewrite the following loop
        # using the walrus operator, like in the Stack Overflow answer linked
        # above.
        bits_per_chunk = self.num_hashes()
        while True:
            bits_in_chunk = tuple(itertools.islice(bits, bits_per_chunk))
            if not bits_in_chunk:
                break
            yield all(bits_in_chunk)

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __contains_many = contains_many

    def __bit_offsets_many(self,
                           *values: JSONTypes,
                           ) -> Generator[int, None, None]:
        for value in values:
            try:
                yield from self._bit_offsets(value)
            except TypeError:
                # value can't be encoded / converted to JSON.  Do a membership
                # test for a UUID in place of value.
                uuid_ = str(uuid.uuid4())
                yield from self._bit_offsets(uuid_)

    def _num_bits_set(self) -> int:
        '''The number of bits set to 1 in this Bloom filter.  O(m)

        Here, m is the size in bits of the underlying string representing this
        Bloom filter.
        '''
        return self.redis.bitcount(self.key)

    def __repr__(self) -> str:
        'Return the string representation of the BloomFilter.  O(1)'
        return f'<{self.__class__.__name__} key={self.key}>'


if __name__ == '__main__':
    # Run the doctests in this module with:
    #   $ source venv/bin/activate
    #   $ python3 -m pottery.bloom
    #   $ deactivate
    import contextlib
    with contextlib.suppress(ImportError):
        from tests.base import run_doctests  # type: ignore
        run_doctests()
