#-----------------------------------------------------------------------------#
#   test_nextid.py                                                            #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#
'Distributed Redis-powered monotonically increasing ID generator tests.'



from pottery import NextId
from tests.base import TestCase



class NextIdTests(TestCase):
    'Distributed Redis-powered monotonically increasing ID generator tests.'

    def setUp(self):
        super().setUp()
        self.ids = NextId()
        for master in self.ids.masters:
            master.set(self.ids.key, 0)

    def test_nextid(self):
        for id_ in range(1, 10):
            with self.subTest(id_=id_):
                assert next(self.ids) == id_
