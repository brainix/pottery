#-----------------------------------------------------------------------------#
#   test_nextid.py                                                            #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#
'Distributed Redis-powered monotonically increasing ID generator tests.'



from redis import Redis

from pottery import NextId
from tests.base import TestCase



class NextIdTests(TestCase):
    'Distributed Redis-powered monotonically increasing ID generator tests.'

    def setUp(self):
        super().setUp()
        self.redis = Redis()
        self.redis.delete(NextId.KEY)

    def test_nextid(self):
        assert not self.redis.exists(NextId.KEY)
        ids = NextId()
        assert int(self.redis.get(NextId.KEY)) == 0
        for id_ in range(1, 10):
            with self.subTest(id_=id_):
                assert next(ids) == id_
