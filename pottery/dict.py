# --------------------------------------------------------------------------- #
#   dict.py                                                                   #
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


import collections.abc
import contextlib
import itertools
import warnings
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Union
from typing import cast

from redis import Redis
from redis.client import Pipeline

from .base import Base
from .base import Iterable_
from .base import JSONTypes
from .exceptions import InefficientAccessWarning
from .exceptions import KeyExistsError


InitMap = Mapping[JSONTypes, JSONTypes]
InitItem = Tuple[JSONTypes, JSONTypes]
InitIter = Iterable[InitItem]
InitArg = Union[InitMap, InitIter]


class RedisDict(Base, Iterable_, collections.abc.MutableMapping):
    'Redis-backed container compatible with Python dicts.'

    def __init__(self,
                 arg: InitArg = tuple(),
                 *,
                 redis: Optional[Redis] = None,
                 key: Optional[str] = None,
                 **kwargs: JSONTypes,
                 ) -> None:
        'Initialize the RedisDict.  O(n)'
        super().__init__(redis=redis, key=key)
        if arg or kwargs:
            with self._watch(arg) as pipeline:
                if pipeline.exists(self.key):
                    raise KeyExistsError(self.redis, self.key)
                self._populate(pipeline, arg, **kwargs)

    def _populate(self,
                  pipeline: Pipeline,
                  arg: InitArg = tuple(),
                  **kwargs: JSONTypes,
                  ) -> None:
        to_set = {}
        with contextlib.suppress(AttributeError):
            arg = cast(InitMap, arg).items()
        for key, value in itertools.chain(cast(InitIter, arg), kwargs.items()):
            to_set[self._encode(key)] = self._encode(value)
        if to_set:
            pipeline.multi()
            pipeline.hset(self.key, mapping=to_set)  # type: ignore

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __populate = _populate

    # Methods required by collections.abc.MutableMapping:

    def __getitem__(self, key: JSONTypes) -> JSONTypes:
        'd.__getitem__(key) <==> d[key].  O(1)'
        encoded_value = self.redis.hget(self.key, self._encode(key))
        if encoded_value is None:
            raise KeyError(key)
        return self._decode(encoded_value)

    def __setitem__(self, key: JSONTypes, value: JSONTypes) -> None:
        'd.__setitem__(key, value) <==> d[key] = value.  O(1)'
        self.redis.hset(self.key, self._encode(key), self._encode(value))

    def __delitem__(self, key: JSONTypes) -> None:
        'd.__delitem__(key) <==> del d[key].  O(1)'
        if not self.redis.hdel(self.key, self._encode(key)):
            raise KeyError(key)

    def _scan(self,
              *,
              cursor: int = 0,
              ) -> Tuple[int, Dict[bytes, bytes]]:
        warnings.warn(
            cast(str, InefficientAccessWarning.__doc__),
            InefficientAccessWarning,
        )
        return self.redis.hscan(self.key, cursor=cursor)

    def __len__(self) -> int:
        'Return the number of items in the RedisDict.  O(1)'
        return self.redis.hlen(self.key)

    # Methods required for Raj's sanity:

    def __repr__(self) -> str:
        'Return the string representation of the RedisDict.  O(n)'
        return self.__class__.__name__ + str(self.to_dict())

    # Method overrides:

    # From collections.abc.MutableMapping:
    def update(self, arg: InitArg = tuple(), **kwargs: JSONTypes) -> None:  # type: ignore
        with self._watch(arg) as pipeline:
            self.__populate(pipeline, arg, **kwargs)

    # From collections.abc.Mapping:
    def __contains__(self, key: Any) -> bool:
        'd.__contains__(key) <==> key in d.  O(1)'
        try:
            return self.redis.hexists(self.key, self._encode(key))
        except TypeError:
            return False

    def to_dict(self) -> Dict[JSONTypes, JSONTypes]:
        warnings.warn(
            cast(str, InefficientAccessWarning.__doc__),
            InefficientAccessWarning,
        )
        items = self.redis.hgetall(self.key).items()
        dict_ = {self._decode(key): self._decode(value) for key, value in items}
        return dict_
