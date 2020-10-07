# --------------------------------------------------------------------------- #
#   test_timer.py                                                             #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import time

from pottery import ContextTimer
from tests.base import TestCase  # type: ignore


class ContextTimerTests(TestCase):
    ACCURACY = 50   # in milliseconds

    def setUp(self):
        super().setUp()
        self.timer = ContextTimer()

    def _confirm_elapsed(self, expected):
        got = round(self.timer.elapsed() / self.ACCURACY) * self.ACCURACY
        assert got == expected, f'{got} != {expected}'

    def test_start_stop_and_elapsed(self):
        # timer hasn't been started
        with self.assertRaises(RuntimeError):
            self.timer.elapsed()
        with self.assertRaises(RuntimeError):
            self.timer.stop()

        # timer has been started but not stopped
        self.timer.start()
        with self.assertRaises(RuntimeError):
            self.timer.start()
        time.sleep(0.1)
        self._confirm_elapsed(1*100)
        self.timer.stop()

        # timer has been stopped
        with self.assertRaises(RuntimeError):
            self.timer.start()
        time.sleep(0.1)
        self._confirm_elapsed(1*100)
        with self.assertRaises(RuntimeError):
            self.timer.stop()

    def test_context_manager(self):
        with self.timer:
            self._confirm_elapsed(0)
            for iteration in range(1, 3):
                with self.subTest(iteration=iteration):
                    time.sleep(0.1)
                    self._confirm_elapsed(iteration*100)
            self._confirm_elapsed(iteration*100)
        time.sleep(0.1)
        self._confirm_elapsed(iteration*100)

        with self.assertRaises(RuntimeError), self.timer:  # pragma: no cover
            ...
