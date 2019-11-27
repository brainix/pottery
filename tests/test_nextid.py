# --------------------------------------------------------------------------- #
#   test_nextid.py                                                            #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #
'Distributed Redis-powered monotonically increasing ID generator tests.'


import unittest.mock

from redis.exceptions import TimeoutError

from pottery import NextId
from pottery import QuorumNotAchieved
from tests.base import TestCase


class NextIdTests(TestCase):
    'Distributed Redis-powered monotonically increasing ID generator tests.'

    def setUp(self):
        super().setUp()
        self.redis.delete('nextid:current')
        self.ids = NextId()
        for master in self.ids.masters:
            master.set(self.ids.key, 0)

    def tearDown(self):
        self.redis.delete('nextid:current')
        super().tearDown()

    def test_nextid(self):
        for id_ in range(1, 10):
            with self.subTest(id_=id_):
                assert next(self.ids) == id_

    def test_iter(self):
        assert iter(self.ids) is self.ids

    def test_next(self):
        with self.assertRaises(QuorumNotAchieved), \
             unittest.mock.patch.object(
                 next(iter(self.ids.masters)),
                 'get',
             ) as get:
            get.side_effect = TimeoutError
            next(self.ids)

        with self.assertRaises(QuorumNotAchieved), \
             unittest.mock.patch.object(
                 self.ids,
                 '_set_id_script',
             ) as _set_id_script:
            _set_id_script.side_effect = TimeoutError
            next(self.ids)

    def test_repr(self):
        assert repr(self.ids) == '<NextId key=nextid:current value=0>'
