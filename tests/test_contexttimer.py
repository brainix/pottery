#-----------------------------------------------------------------------------#
#   test_contexttimer.py                                                      #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import time

from redis import Redis

from pottery import contexttimer
from tests.base import TestCase



class ContextTimerTests(TestCase):
    ACCURACY = 50   # in milliseconds

    def _confirm_elapsed(self, timer, expected):
        got = round(timer.elapsed / self.ACCURACY) * self.ACCURACY
        assert got == expected, '{} != {}'.format(got, expected)

    def test_contexttimer(self):
        with contexttimer() as timer:
            self._confirm_elapsed(timer, 0)
            for iteration in range(1, 3):
                with self.subTest(iteration=iteration):
                    time.sleep(0.1)
                    self._confirm_elapsed(timer, iteration*100)
            self._confirm_elapsed(timer, iteration*100)
        self._confirm_elapsed(timer, iteration*100)
