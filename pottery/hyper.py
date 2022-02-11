# --------------------------------------------------------------------------- #
#   hyper.py                                                                  #
#                                                                             #
#   Copyright © 2015-2022, Rajiv Bakulesh Shah, original author.              #
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


# TODO: Remove the following import after deferred evaluation of annotations
# because the default.
#   1. https://docs.python.org/3/whatsnew/3.7.html#whatsnew37-pep563
#   2. https://www.python.org/dev/peps/pep-0563/
#   3. https://www.python.org/dev/peps/pep-0649/
from __future__ import annotations

import uuid
from typing import Generator
from typing import Iterable
from typing import List
from typing import cast

from redis import Redis

from .annotations import JSONTypes
from .annotations import RedisValues
from .base import Container
from .base import random_key


class HyperLogLog(Container):
    '''Redis-backed HyperLogLog with a Pythonic API.

    HyperLogLogs are an interesting data structure designed to answer the
    question, "How many distinct elements have I seen?"; but not the questions,
    "Have I seen this element before?" or "What are all of the elements that
    I've seen before?"  So think of HyperLogLogs as Python sets that you can add
    elements to and get the length of; but that you might not want to use to
    test element membership, and can't iterate through, or get elements out of.

    HyperLogLogs are probabilistic, which means that they&rsquo;re accurate
    within a margin of error up to 2%.  However, they can reasonably accurately
    estimate the cardinality (size) of vast datasets (like the number of unique
    Google searches issued in a day) with a tiny amount of storage (1.5 KB).

    Wikipedia article:
        https://en.wikipedia.org/wiki/HyperLogLog

    antirez's blog post:
        http://antirez.com/news/75

    Riak blog post:
        https://riak.com/posts/technical/what-in-the-hell-is-hyperloglog/index.html?p=13169.html

    Create a HyperLogLog and clean up Redis before the doctest:

        >>> google_searches = HyperLogLog(key='google-searches')
        >>> google_searches.clear()

    Insert an element into the HyperLogLog:

        >>> google_searches.add('sonic the hedgehog video game')

    See how many elements we've inserted into the HyperLogLog:

        >>> len(google_searches)
        1

    Insert multiple elements into the HyperLogLog:

        >>> google_searches.update({
        ...     'google in 1998',
        ...     'minesweeper',
        ...     'joey tribbiani',
        ...     'wizard of oz',
        ...     'rgb to hex',
        ...     'pac-man',
        ...     'breathing exercise',
        ...     'do a barrel roll',
        ...     'snake',
        ... })
        >>> len(google_searches)
        10

    Test for element membership in the HyperLogLog:

        >>> 'joey tribbiani' in google_searches
        True
        >>> 'jennifer aniston' in google_searches
        False

    Remove all of the elements from the HyperLogLog:

        >>> google_searches.clear()
    '''

    def __init__(self,
                 iterable: Iterable[RedisValues] = frozenset(),
                 *,
                 redis: Redis | None = None,
                 key: str = '',
                 ) -> None:
        '''Initialize the HyperLogLog.  O(n)

        Here, n is the number of elements in iterable that you want to insert
        into this HyperLogLog.
        '''
        super().__init__(redis=redis, key=key)
        self.__update(iterable)

    def add(self, value: RedisValues) -> None:
        'Add an element to the HyperLogLog.  O(1)'
        self.__update({value})

    def update(self, *objs: HyperLogLog | Iterable[RedisValues]) -> None:
        other_hll_keys: List[str] = []
        encoded_values: List[str] = []
        with self._watch(objs) as pipeline:
            for obj in objs:
                if isinstance(obj, self.__class__):
                    if not self._same_redis(obj):
                        raise RuntimeError(
                            f"can't update {self} with {obj} as they live on "
                            "different Redis instances/databases"
                        )
                    other_hll_keys.append(obj.key)
                else:
                    for value in cast(Iterable[JSONTypes], obj):
                        encoded_value = self._encode(value)
                        encoded_values.append(encoded_value)
            pipeline.multi()  # Available since Redis 1.2.0
            pipeline.pfmerge(self.key, *other_hll_keys)  # Available since Redis 2.8.9
            pipeline.pfadd(self.key, *encoded_values)  # Available since Redis 2.8.9

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __update = update

    def union(self,
              *objs: Iterable[RedisValues],
              redis: Redis | None = None,
              key: str = '',
              ) -> HyperLogLog:
        new_hll = self.__class__(redis=redis, key=key)
        new_hll.update(self, *objs)
        return new_hll

    def __len__(self) -> int:
        '''Return the approximate number of elements in the HyperLogLog.  O(1)

        Please note that this method returns an approximation, not an exact
        value, though it's quite accurate.
        '''
        return self.redis.pfcount(self.key)  # Available since Redis 2.8.9

    def __contains__(self, value: JSONTypes) -> bool:
        '''hll.__contains__(element) <==> element in hll.  O(1)

        Please note that this method *may* return false positives, but *never*
        returns false negatives.  This means that if `element in hll` evaluates
        to True, then you *may* have inserted the element into the HyperLogLog.
        But if `element in hll` evaluates to False, then you *must not* have
        inserted it.
        '''
        return next(self.__contains_many(value))

    def contains_many(self, *values: JSONTypes) -> Generator[bool, None, None]:
        '''Yield whether this HyperLogLog contains multiple elements.  O(n)

        Please note that this method *may* produce false positives, but *never*
        produces false negatives.  This means that if .contains_many() yields
        True, then you *may* have inserted the element into the HyperLogLog.
        But if .contains_many() yields False, then you *must not* have inserted
        it.
        '''
        encoded_values = []
        for value in values:
            try:
                encoded_value = self._encode(value)
            except TypeError:
                encoded_value = str(uuid.uuid4())
            encoded_values.append(encoded_value)

        with self._watch() as pipeline:
            tmp_hll_key = random_key(redis=pipeline)
            pipeline.multi()  # Available since Redis 1.2.0
            for encoded_value in encoded_values:
                pipeline.copy(self.key, tmp_hll_key)  # Available since Redis 6.2.0
                pipeline.pfadd(tmp_hll_key, encoded_value)  # Available since Redis 2.8.9
                pipeline.unlink(tmp_hll_key)  # Available since Redis 4.0.0
            # Pluck out the results of the pipeline.pfadd() commands.  Ignore
            # the results of the enclosing pipeline.copy() and pipeline.unlink()
            # commands.
            cardinalities_changed = pipeline.execute()[1::3]  # Available since Redis 1.2.0

        for cardinality_changed in cardinalities_changed:
            yield not cardinality_changed

    __contains_many = contains_many

    def __repr__(self) -> str:
        'Return the string representation of the HyperLogLog.  O(1)'
        return f'<{self.__class__.__name__} key={self.key} len={len(self)}>'


if __name__ == '__main__':
    # Run the doctests in this module with:
    #   $ source venv/bin/activate
    #   $ python3 -m pottery.bloom
    #   $ deactivate
    import contextlib
    with contextlib.suppress(ImportError):
        from tests.base import run_doctests
        run_doctests()
