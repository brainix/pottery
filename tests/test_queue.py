# --------------------------------------------------------------------------- #
#   test_queue.py                                                             #
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


from pottery import ContextTimer
from pottery import QueueEmptyError
from pottery import RedisSimpleQueue
from tests.base import TestCase


class QueueTests(TestCase):
    def test_put(self):
        queue = RedisSimpleQueue()

        assert queue.qsize() == 0
        assert queue.empty()

        for num in range(1, 6):
            with self.subTest(num=num):
                queue.put(num)
                assert queue.qsize() == num
                assert not queue.empty()

    def test_put_nowait(self):
        queue = RedisSimpleQueue()

        assert queue.qsize() == 0
        assert queue.empty()

        for num in range(1, 6):
            with self.subTest(num=num):
                queue.put_nowait(num)
                assert queue.qsize() == num
                assert not queue.empty()

    def test_get(self):
        queue = RedisSimpleQueue()

        with self.assertRaises(QueueEmptyError):
            queue.get()

        for num in range(1, 6):
            with self.subTest(num=num):
                queue.put(num)
                assert queue.get() == num
                assert queue.qsize() == 0
                assert queue.empty()

        with self.assertRaises(QueueEmptyError):
            queue.get()

    def test_get_nowait(self):
        queue = RedisSimpleQueue()

        with self.assertRaises(QueueEmptyError):
            queue.get_nowait()

        for num in range(1, 6):
            queue.put(num)

        for num in range(1, 6):
            with self.subTest(num=num):
                assert queue.get_nowait() == num
                assert queue.qsize() == 5 - num
                assert queue.empty() == (num == 5)

        with self.assertRaises(QueueEmptyError):
            queue.get_nowait()

    def test_get_timeout(self):
        queue = RedisSimpleQueue()
        timeout = 1

        with self.assertRaises(QueueEmptyError), ContextTimer() as timer:
            queue.get(timeout=timeout)
        assert timer.elapsed() / 1000 >= timeout
