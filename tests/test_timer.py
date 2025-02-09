# --------------------------------------------------------------------------- #
#   test_timer.py                                                             #
#                                                                             #
#   Copyright © 2015-2025, Rajiv Bakulesh Shah, original author.              #
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

import pytest

from pottery import ContextTimer


@pytest.fixture
def timer() -> ContextTimer:
    return ContextTimer()


def confirm_elapsed(timer: ContextTimer, expected: int) -> None:
    ACCURACY = 50   # in milliseconds
    elapsed = timer.elapsed()
    assert elapsed >= expected, f'elapsed ({elapsed}) is not >= expected ({expected})'
    assert elapsed < expected + ACCURACY, f'elapsed ({elapsed}) is not < expected ({expected + ACCURACY})'


def test_start_stop_and_elapsed(timer: ContextTimer) -> None:
    # timer hasn't been started
    with pytest.raises(RuntimeError):
        timer.elapsed()
    with pytest.raises(RuntimeError):
        timer.stop()

    # timer has been started but not stopped
    timer.start()
    with pytest.raises(RuntimeError):
        timer.start()
    time.sleep(0.1)
    confirm_elapsed(timer, 1*100)
    timer.stop()

    # timer has been stopped
    with pytest.raises(RuntimeError):
        timer.start()
    time.sleep(0.1)
    confirm_elapsed(timer, 1*100)
    with pytest.raises(RuntimeError):
        timer.stop()


def test_context_manager(timer: ContextTimer) -> None:
    with timer:
        confirm_elapsed(timer, 0)
        for iteration in range(1, 3):
            time.sleep(0.1)
            confirm_elapsed(timer, iteration*100)
        confirm_elapsed(timer, iteration*100)
    time.sleep(0.1)
    confirm_elapsed(timer, iteration*100)

    with pytest.raises(RuntimeError), timer:  # pragma: no cover
        ...
