# --------------------------------------------------------------------------- #
#   queue.py                                                                  #
#                                                                             #
#   Copyright © 2015-2022, Rajiv Bakulesh Shah, original author.              #
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


# TODO: When we drop support for Python 3.9, remove the following import.  We
# only need it for X | Y union type annotations as of 2022-01-29.
from __future__ import annotations

import math
import random
import time
from typing import ClassVar
from typing import Tuple
from typing import cast

from redis import WatchError

from .annotations import JSONTypes
from .base import Container
from .exceptions import QueueEmptyError
from .timer import ContextTimer


class RedisSimpleQueue(Container):
    RETRY_DELAY: ClassVar[int] = 200

    def qsize(self) -> int:
        '''Return the size of the queue.  O(1)

        Be aware that there's a potential race condition here where the queue
        changes before you use the result of .qsize().
        '''
        return self.redis.xlen(self.key)  # Available since Redis 5.0.0

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __qsize = qsize

    def empty(self) -> bool:
        '''Return True if the queue is empty; False otherwise.  O(1)

        This method is likely to be removed at some point.  Use `.qsize() == 0`
        as a direct substitute, but be aware that either approach risks a race
        condition where the queue grows before you use the result of .empty() or
        .qsize().
        '''
        return self.__qsize() == 0

    def put(self,
            item: JSONTypes,
            block: bool = True,
            timeout: float | None = None,
            ) -> None:
        '''Put the item on the queue.  O(1)

        The optional block and timeout arguments are ignored, as this method
        never blocks.  They are provided for compatibility with the queue.Queue
        class.
        '''
        encoded_item = self._encode(item)
        self.redis.xadd(self.key, {'item': encoded_item}, id='*')  # Available since Redis 5.0.0

    __put = put

    def put_nowait(self, item: JSONTypes) -> None:
        '''Put an item into the queue without blocking.  O(1)

        This is exactly equivalent to `.put(item)` and is provided for
        compatibility with the queue.Queue class.
        '''
        return self.__put(item, block=False)

    def get(self,
            block: bool = True,
            timeout: float | None = None,
            ) -> JSONTypes:
        '''Remove and return an item from the queue.  O(1)

        If optional args block is True and timeout is None (the default), block
        if necessary until an item is available.  If timeout is a non-negative
        number, block at most timeout seconds and raise the QueueEmptyError
        exception if no item becomes available within that time.  Otherwise
        (block is False), return an item if one is immediately available, else
        raise the QueueEmptyError exception (timeout is ignored in this case).
        '''
        redis_block = (timeout or 0.0) if block else 0.0
        redis_block = math.floor(redis_block)
        with ContextTimer() as timer:
            while True:
                try:
                    item = self.__remove_and_return(redis_block)
                    return item
                except (WatchError, IndexError):
                    if not block or timer.elapsed() / 1000 >= (timeout or 0):
                        raise QueueEmptyError(redis=self.redis, key=self.key)
                    delay = random.uniform(0, self.RETRY_DELAY/1000)  # nosec
                    time.sleep(delay)

    __get = get

    def __remove_and_return(self, redis_block: int) -> JSONTypes:
        with self._watch() as pipeline:
            # XXX: The following line raises WatchError after the socket timeout
            # if the RedisQueue is empty and we're not blocking.  This feels
            # like a bug in redis-py?
            returned_value = pipeline.xread(  # Available since Redis 5.0.0
                {self.key: 0},
                count=1,
                block=redis_block,
            )
            # The following line raises IndexError if the RedisQueue is empty
            # and we're blocking.
            id_, dict_ = cast(Tuple[bytes, dict], returned_value[0][1][0])
            pipeline.multi()  # Available since Redis 1.2.0
            pipeline.xdel(self.key, id_)  # Available since Redis 5.0.0
        encoded_item = dict_[b'item']
        item = self._decode(encoded_item)
        return item

    def get_nowait(self) -> JSONTypes:
        '''Remove and return an item from the queue without blocking.  O(1)

        Get an item if one is immediately available.  Otherwise raise the
        QueueEmptyError exception.

        This is exactly equivalent to `.get(block=False)` and is provided for
        compatibility with the queue.Queue class.
        '''
        return self.__get(block=False)
