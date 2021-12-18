# --------------------------------------------------------------------------- #
#   hyper.py                                                                  #
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


import contextlib
import uuid
from typing import Generator
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union
from typing import cast

from redis import Redis

from .annotations import RedisValues
from .base import Base
from .base import JSONTypes
from .base import random_key


class HyperLogLog(Base):
    '''Redis-backed HyperLogLog with a Pythonic API.

    Wikipedia article:
        https://en.wikipedia.org/wiki/HyperLogLog

    antirez's blog post:
        http://antirez.com/news/75

    Riak blog post:
        https://riak.com/posts/technical/what-in-the-hell-is-hyperloglog/index.html?p=13169.html
    '''

    def __init__(self,
                 iterable: Iterable[RedisValues] = frozenset(),
                 *,
                 redis: Optional[Redis] = None,
                 key: Optional[str] = None,
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

    def update(self,
               *objs: Union['HyperLogLog', Iterable[RedisValues]],
               ) -> None:
        other_hll_keys: List[str] = []
        encoded_values: List[str] = []
        with self._watch(objs) as pipeline:
            for obj in objs:
                if isinstance(obj, self.__class__):
                    if self.redis.connection_pool != obj.redis.connection_pool:
                        raise RuntimeError(
                            f"can't update {self} with {obj} as they live on "
                            "different Redis instances/databases"
                        )
                    other_hll_keys.append(obj.key)
                else:
                    for decoded_value in cast(Iterable[JSONTypes], obj):
                        encoded_value = self._encode(decoded_value)
                        encoded_values.append(encoded_value)
            pipeline.multi()
            pipeline.pfmerge(self.key, *other_hll_keys)
            pipeline.pfadd(self.key, *encoded_values)

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __update = update

    def union(self,
              *objs: Iterable[RedisValues],
              redis: Optional[Redis] = None,
              key: Optional[str] = None,
              ) -> 'HyperLogLog':
        new_hll = self.__class__(redis=redis, key=key)
        new_hll.update(self, *objs)
        return new_hll

    def __len__(self) -> int:
        '''Return the approximate number of elements in the HyperLogLog.  O(1)

        Please note that this method returns an approximation, not an exact
        value.  So please don't rely on it for anything important like
        financial systems or cat gif websites.
        '''
        return self.redis.pfcount(self.key)

    def __contains__(self, value: JSONTypes) -> bool:
        'hll.__contains__(element) <==> element in hll.  O(1)'
        return next(self.__contains_many(value))

    def contains_many(self, *values: JSONTypes) -> Generator[bool, None, None]:
        'Yield whether this HyperLogLog contains multiple elements.  O(n)'

        # Create a temporary copy of this HyperLogLog:
        with self.__tmp_key() as tmp_key:

            # Encode the elements:
            encoded_values = []
            for value in values:
                try:
                    encoded_value = self._encode(value)
                except TypeError:
                    uuid_ = str(uuid.uuid4())
                    encoded_value = self._encode(uuid_)
                encoded_values.append(encoded_value)

            # Insert the encoded elements one by one into the temporary
            # HyperLogLog:
            pipeline = self.redis.pipeline()
            for encoded_value in encoded_values:
                pipeline.pfadd(tmp_key, encoded_value)

            # After each insertion, if the cardinality of the temporary
            # HyperLogLog changes, then the element must not have been in this
            # HyperLogLog.
            cardinalities_changed = pipeline.execute()
            for cardinality_changed in cardinalities_changed:
                yield not cardinality_changed

    __contains_many = contains_many

    @contextlib.contextmanager
    def __tmp_key(self):
        # Create a yield a tmp copy of this HLL; finally, delete the tmp HLL.
        tmp_key = random_key(redis=self.redis)
        self.redis.copy(self.key, tmp_key)  # type: ignore
        try:
            yield tmp_key
        finally:
            self.redis.delete(tmp_key)

    def __repr__(self) -> str:
        'Return the string representation of the HyperLogLog.  O(1)'
        return f'<{self.__class__.__name__} key={self.key} len={len(self)}>'
