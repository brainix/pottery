#-----------------------------------------------------------------------------#
#   hyper.py                                                                  #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from .base import Base



class HyperLogLog(Base):
    '''Redis-backed HyperLogLog with a Pythonic API.

    Wikipedia article:
        https://en.wikipedia.org/wiki/HyperLogLog

    antirez's blog post:
        http://antirez.com/news/75
    '''

    def __init__(self, iterable=frozenset(), *, redis=None, key=None):
        '''Initialize a HyperLogLog.  O(n)

        Here, n is the number of elements in iterable that you want to insert
        into this HyperLogLog.
        '''
        super().__init__(redis=redis, key=key)
        self.update(iterable)

    def add(self, value):
        'Add an element to a HyperLogLog.  O(1)'
        self.update({value})

    def update(self, *objs):
        objs, other_hll_keys, encoded_values = (self,) + tuple(objs), [], []
        for obj in objs:
            if isinstance(obj, self.__class__):
                other_hll_keys.append(obj.key)
            else:
                encoded_values.extend(self._encode(value) for value in obj)
        with self._watch(objs[1:]):
            self.redis.multi()
            self.redis.pfmerge(self.key, *other_hll_keys)
            self.redis.pfadd(self.key, *encoded_values)

    def union(self, *objs, redis=None, key=None):
        new_hll = self.__class__(redis=redis, key=key)
        new_hll.update(self, *objs)
        return new_hll

    def __len__(self):
        '''Return the approximate number of elements in a HyperLogLog.  O(1)

        Please note that this method returns an approximation, not an exact
        value.  So please don't rely on it for anything important like
        financial systems or cat gif websites.
        '''
        return self.redis.pfcount(self.key)

    def __repr__(self):
        'Return the string representation of a HyperLogLog.  O(1)'
        return '<{} key={} len={}>'.format(
            self.__class__.__name__,
            self.key,
            len(self),
        )
