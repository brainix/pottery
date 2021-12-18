# --------------------------------------------------------------------------- #
#   test_timer.py                                                             #
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


import time

from pottery import ContextTimer
from tests.base import TestCase


class ContextTimerTests(TestCase):
    _ACCURACY = 50   # in milliseconds

    def setUp(self):
        super().setUp()
        self.timer = ContextTimer()

    def _confirm_elapsed(self, expected):
        elapsed = self.timer.elapsed()
        assert elapsed >= expected, f'elapsed ({elapsed}) is not >= expected ({expected})'
        assert elapsed < expected + self._ACCURACY, f'elapsed ({elapsed}) is not < expected ({expected + self._ACCURACY})'

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
