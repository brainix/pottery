#-----------------------------------------------------------------------------#
#   bloom.py                                                                  #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import itertools
import math

import mmh3

from .base import Base



class BloomFilter(Base):
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
        ...     num_values=100,
        ...     false_positives=0.01,
        ...     key='dilberts',
        ... )
        >>> dilberts.clear()

    Here, num_values represents the number of elements that you expect to
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

    Note that BloomFilter.__len__() is an approximation, so please don't rely
    on it for anything important like financial systems or cat gif websites.

    Insert multiple elements into the Bloom filter:

        >>> dilberts.update({'raj', 'dan'})

    Remove all of the elements from the Bloom filter:

        >>> dilberts.clear()
    '''

    def __init__(self, iterable=frozenset(), *, num_values, false_positives,
                 redis=None, key=None):
        '''Initialize a BloomFilter.  O(n * k)

        Here, n is the number of elements in iterable that you want to insert
        into this Bloom filter, and k is the number of times to run our hash
        functions on each element.
        '''
        super().__init__(redis=redis, key=key)
        self.num_values = num_values
        self.false_positives = false_positives
        self.update(iterable)

    def size(self):
        '''The required number of bits (m) given n and p.

        This method returns the required number of bits (m) for the underlying
        string representing this Bloom filter given the the number of elements
        that you expect to insert (n) and your acceptable false positive
        probability (p).

        More about the formula that this method implements:
            https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
        '''
        try:
            return self._size
        except AttributeError:
            self._size = (
                -self.num_values *
                math.log(self.false_positives) /
                math.log(2)**2
            )
            self._size = math.ceil(self._size)
            return self.size()

    def num_hashes(self):
        '''The number of hash functions (k) given m and n, minimizing p.

        This method returns the number of times (k) to run our hash functions
        on a given input string to compute bit offsets into the underlying
        string representing this Bloom filter.  m is the size in bits of the
        underlying string, n is the number of elements that you expect to
        insert, and p is your acceptable false positive probability.

        More about the formula that this method implements:
            https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
        '''
        try:
            return self._num_hashes
        except AttributeError:
            self._num_hashes = self.size() / self.num_values * math.log(2)
            self._num_hashes = math.ceil(self._num_hashes)
            return self.num_hashes()

    def _bit_offsets(self, value):
        '''The bit offsets to set/check in this Bloom filter for a given value.

        Instantiate a Bloom filter:

            >>> dilberts = BloomFilter(
            ...     num_values=100,
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
        was *probably* inserted into our Bloom filter.
        '''
        encoded_value = self._encode(value)
        for seed in range(self.num_hashes()):
            yield mmh3.hash(encoded_value, seed=seed) % self.size()

    def update(self, *iterables):
        '''Populate a Bloom filter with the elements in iterables.  O(n * k)

        Here, n is the number of elements in iterables that you want to insert
        into this Bloom filter, and k is the number of times to run our hash
        functions on each element.
        '''
        iterables, bit_offsets = tuple(iterables), set()
        for value in itertools.chain(*iterables):
            bit_offsets.update(self._bit_offsets(value))

        with self._watch(iterables):
            self.redis.multi()
            for bit_offset in bit_offsets:
                self.redis.setbit(self.key, bit_offset, 1)

    def __contains__(self, value):
        '''bf.__contains__(element) <==> element in bf.  O(k)

        Here, k is the number of times to run our hash functions on a given
        input string to compute bit offests into the underlying string
        representing this Bloom filter.
        '''
        bit_offsets = set(self._bit_offsets(value))

        with self._watch():
            self.redis.multi()
            for bit_offset in bit_offsets:
                self.redis.getbit(self.key, bit_offset)
            bits = self.redis.execute()
        return all(bits)

    def add(self, value):
        '''Add an element to a BloomFilter.  O(k)

        Here, k is the number of times to run our hash functions on a given
        input string to compute bit offests into the underlying string
        representing this Bloom filter.
        '''
        self.update({value})

    def _num_bits_set(self):
        '''The number of bits set to 1 in this Bloom filter.  O(m)

        Here, m is the size in bits of the underlying string representing this
        Bloom filter.
        '''
        return self.redis.bitcount(self.key)

    def __len__(self):
        '''Return the approximate the number of elements in a BloomFilter.  O(m)

        Here, m is the size in bits of the underlying string representing this
        Bloom filter.

        Please note that this method returns an approximation, not an exact
        value.  So please don't rely on it for anything important like
        financial systems or cat gif websites.

        More about the formula that this method implements:
            https://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter
        '''
        len_ = (
            -self.size() /
            self.num_hashes() *
            math.log(1 - self._num_bits_set() / self.size())
        )
        return math.floor(len_)

    def __repr__(self):
        'Return the string representation of a BloomFilter.  O(1)'
        return '<{} key={}>'.format(self.__class__.__name__, self.key)



if __name__ == '__main__':  # pragma: no cover
    # Run the doctests in this module with:
    #   $ source venv/bin/activate
    #   $ python3 -m pottery.bloom
    #   $ deactivate
    import contextlib
    with contextlib.suppress(ImportError):
        from tests.base import run_doctests
        run_doctests()
