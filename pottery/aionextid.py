# --------------------------------------------------------------------------- #
#   aionextid.py                                                              #
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
'Asynchronous distributed Redis-powered monotonically increasing ID generator.'


# TODO: Remove the following import after deferred evaluation of annotations
# because the default.
#   1. https://docs.python.org/3/whatsnew/3.7.html#whatsnew37-pep563
#   2. https://www.python.org/dev/peps/pep-0563/
#   3. https://www.python.org/dev/peps/pep-0649/
from __future__ import annotations

from typing import ClassVar
from typing import Iterable

from redis.asyncio import Redis as AIORedis  # type: ignore

from .base import AIOPrimitive
from .nextid import NextID
from .nextid import Scripts


class AIONextID(Scripts, AIOPrimitive):
    'Async distributed Redis-powered monotonically increasing ID generator.'

    __slots__ = ('num_tries',)

    _KEY_PREFIX: ClassVar[str] = NextID._KEY_PREFIX

    def __init__(self,  # type: ignore
                 *,
                 key: str = 'current',
                 masters: Iterable[AIORedis] = frozenset(),
                 num_tries: int = NextID._NUM_TRIES,
                 ) -> None:
        'Initialize an AIONextID ID generator.'
        super().__init__(key=key, masters=masters)
        self.num_tries = num_tries
