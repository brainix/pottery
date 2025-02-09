# --------------------------------------------------------------------------- #
#   test_queue.py                                                             #
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


import pytest
from redis import Redis

from pottery import ContextTimer
from pottery import QueueEmptyError
from pottery import RedisSimpleQueue


@pytest.fixture
def queue(redis: Redis) -> RedisSimpleQueue:
    return RedisSimpleQueue(redis=redis)


def test_put(queue: RedisSimpleQueue) -> None:
    assert queue.qsize() == 0
    assert queue.empty()

    for num in range(1, 6):
        queue.put(num)
        assert queue.qsize() == num
        assert not queue.empty()


def test_put_nowait(queue: RedisSimpleQueue) -> None:
    assert queue.qsize() == 0
    assert queue.empty()

    for num in range(1, 6):
        queue.put_nowait(num)
        assert queue.qsize() == num
        assert not queue.empty()


def test_get(queue: RedisSimpleQueue) -> None:
    with pytest.raises(QueueEmptyError):
        queue.get()

    for num in range(1, 6):
        queue.put(num)
        assert queue.get() == num
        assert queue.qsize() == 0
        assert queue.empty()

    with pytest.raises(QueueEmptyError):
        queue.get()


def test_get_nowait(queue: RedisSimpleQueue) -> None:
    with pytest.raises(QueueEmptyError):
        queue.get_nowait()

    for num in range(1, 6):
        queue.put(num)

    for num in range(1, 6):
        assert queue.get_nowait() == num
        assert queue.qsize() == 5 - num
        assert queue.empty() == (num == 5)

    with pytest.raises(QueueEmptyError):
        queue.get_nowait()


def test_get_timeout(queue: RedisSimpleQueue) -> None:
    timeout = 1
    with pytest.raises(QueueEmptyError), ContextTimer() as timer:
        queue.get(timeout=1)
    assert timer.elapsed() / 1000 >= timeout
