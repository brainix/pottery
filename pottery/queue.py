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


from typing import Optional

from .base import Base
from .base import JSONTypes


class RedisSimpleQueue(Base):
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
        ...

    __put = put

    def put_nowait(self, item: JSONTypes) -> None:
        return self.__put(item, False)

    def get(self,
            block: bool = True,
            timeout: Optional[float] = None,
            ) -> JSONTypes:
        ...

    __get = get

    def get_nowait(self) -> JSONTypes:
        return self.__get(False)
