# --------------------------------------------------------------------------- #
#   queue.py                                                                  #
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


import math
import random
import time
from typing import ClassVar
from typing import Optional
from typing import cast

from redis import WatchError

from .base import Base
from .base import JSONTypes
from .exceptions import QueueEmptyError
from .timer import ContextTimer


class RedisSimpleQueue(Base):
    RETRY_DELAY: ClassVar[int] = 200

    def qsize(self) -> int:
        return self.redis.xlen(self.key)

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __qsize = qsize

    def empty(self) -> bool:
        return self.__qsize() == 0

    def put(self,
            item: JSONTypes,
            block: bool = True,
            timeout: Optional[float] = None,
            ) -> None:
        encoded_value = self._encode(item)
        self.redis.xadd(self.key, {'item': encoded_value}, id='*')

    __put = put

    def put_nowait(self, item: JSONTypes) -> None:
        return self.__put(item, False)

    def get(self,
            block: bool = True,
            timeout: Optional[float] = None,
            ) -> JSONTypes:
        redis_block = math.floor((timeout or 0.0) if block else 0.0)
        with ContextTimer() as timer:
            while True:
                try:
                    item = self.__remove_and_return(redis_block)
                    return item
                except (WatchError, IndexError):
                    if not block or timer.elapsed() / 1000 >= (timeout or 0):
                        raise QueueEmptyError(redis=self.redis, key=self.key)
                    delay = random.uniform(0, self.RETRY_DELAY/1000)
                    time.sleep(delay)

    __get = get

    def __remove_and_return(self, redis_block: int) -> JSONTypes:
        with self._watch() as pipeline:
            # The following line raises WatchError if the RedisQueue is empty
            # and we're not blocking.
            returned_value = pipeline.xread({self.key: 0}, count=1, block=redis_block)
            # The following line raises IndexError if the RedisQueue is empty
            # and we're blocking.
            id_ = cast(bytes, returned_value[0][1][0][0])
            dict_ = cast(dict, returned_value[0][1][0][1])
            pipeline.multi()
            pipeline.xdel(self.key, id_)
        encoded_value = dict_[b'item']
        item = self._decode(encoded_value)
        return item

    def get_nowait(self) -> JSONTypes:
        return self.__get(False)
