# --------------------------------------------------------------------------- #
#   hyper.py                                                                  #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


from typing import Iterable
from typing import List
from typing import Optional
from typing import Union
from typing import cast

from redis import Redis

from .base import Base
from .base import JSONTypes
from .base import RedisValues


class HyperLogLog(Base):
    '''Redis-backed HyperLogLog with a Pythonic API.

    Wikipedia article:
        https://en.wikipedia.org/wiki/HyperLogLog

    antirez's blog post:
        http://antirez.com/news/75
    '''

    def __init__(self,
                 iterable: Iterable[RedisValues] = frozenset(),
                 *,
                 redis: Optional[Redis] = None,
                 key: Optional[str] = None,
                 ) -> None:
        '''Initialize a HyperLogLog.  O(n)

        Here, n is the number of elements in iterable that you want to insert
        into this HyperLogLog.
        '''
        super().__init__(redis=redis, key=key)
        self.update(iterable)

    def add(self, value: RedisValues) -> None:
        'Add an element to a HyperLogLog.  O(1)'
        self.update({value})

    def update(self,
               *objs: Union['HyperLogLog', Iterable[RedisValues]],
               ) -> None:
        objs = (self,) + tuple(objs)
        other_hll_keys: List[str] = []
        encoded_values: List[str] = []
        with self._watch(objs[1:]) as pipeline:
            for obj in objs:
                if isinstance(obj, self.__class__):
                    other_hll_keys.append(obj.key)
                else:
                    for value in cast(Iterable[JSONTypes], obj):
                        encoded_values.append(self._encode(value))
            pipeline.multi()
            pipeline.pfmerge(self.key, *other_hll_keys)
            pipeline.pfadd(self.key, *encoded_values)

    def union(self,
              *objs: Iterable[RedisValues],
              redis: Optional[Redis] = None,
              key: Optional[str] = None,
              ) -> 'HyperLogLog':
        new_hll = self.__class__(redis=redis, key=key)
        new_hll.update(self, *objs)
        return new_hll

    def __len__(self) -> int:
        '''Return the approximate number of elements in a HyperLogLog.  O(1)

        Please note that this method returns an approximation, not an exact
        value.  So please don't rely on it for anything important like
        financial systems or cat gif websites.
        '''
        return self.redis.pfcount(self.key)

    def __repr__(self) -> str:
        'Return the string representation of a HyperLogLog.  O(1)'
        return '<{} key={} len={}>'.format(
            self.__class__.__name__,
            self.key,
            len(self),
        )
