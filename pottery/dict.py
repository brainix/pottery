# --------------------------------------------------------------------------- #
#   dict.py                                                                   #
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

import collections.abc
import itertools
import warnings
from typing import Any
from typing import Dict
from typing import Generator
from typing import Iterable
from typing import Mapping
from typing import Tuple
from typing import Union
from typing import cast

from redis import Redis
from redis.client import Pipeline

from .annotations import JSONTypes
from .base import Container
from .base import Iterable_
from .exceptions import InefficientAccessWarning
from .exceptions import KeyExistsError


InitMap = Mapping[JSONTypes, JSONTypes]
InitItem = Tuple[JSONTypes, JSONTypes]
InitIter = Iterable[InitItem]
InitArg = Union[InitMap, InitIter]


class RedisDict(Container, Iterable_, collections.abc.MutableMapping):
    'Redis-backed container compatible with Python dicts.'

    def __init__(self,
                 arg: InitArg = tuple(),
                 *,
                 redis: Redis | None = None,
                 key: str = '',
                 **kwargs: JSONTypes,
                 ) -> None:
        'Initialize the RedisDict.  O(n)'
        super().__init__(redis=redis, key=key)
        if arg or kwargs:
            with self._watch(arg) as pipeline:
                if pipeline.exists(self.key):  # Available since Redis 1.0.0
                    raise KeyExistsError(self.redis, self.key)
                self._populate(pipeline, arg, **kwargs)

    def _populate(self,
                  pipeline: Pipeline,
                  arg: InitArg = tuple(),
                  **kwargs: JSONTypes,
                  ) -> None:
        if isinstance(arg, collections.abc.Mapping):
            arg = arg.items()
        items = itertools.chain(arg, kwargs.items())
        dict_ = dict(items)
        encoded_dict = self.__encode_dict(dict_)
        if encoded_dict:
            pipeline.multi()  # Available since Redis 1.2.0
            # Available since Redis 2.0.0:
            pipeline.hset(self.key, mapping=encoded_dict)  # type: ignore

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __populate = _populate

    def _encode_dict(self, dict_: Mapping[JSONTypes, JSONTypes]) -> Dict[str, str]:
        encoded_dict = {
            self._encode(key): self._encode(value)
            for key, value in dict_.items()
        }
        return encoded_dict

    __encode_dict = _encode_dict

    # Methods required by collections.abc.MutableMapping:

    def __getitem__(self, key: JSONTypes) -> JSONTypes:
        'd.__getitem__(key) <==> d[key].  O(1)'
        encoded_key = self._encode(key)
        encoded_value = self.redis.hget(self.key, encoded_key)  # Available since Redis 2.0.0
        if encoded_value is None:
            raise KeyError(key)
        value = self._decode(encoded_value)
        return value

    def __setitem__(self, key: JSONTypes, value: JSONTypes) -> None:
        'd.__setitem__(key, value) <==> d[key] = value.  O(1)'
        encoded_key = self._encode(key)
        encoded_value = self._encode(value)
        self.redis.hset(self.key, encoded_key, encoded_value)  # Available since Redis 2.0.0

    def __delitem__(self, key: JSONTypes) -> None:
        'd.__delitem__(key) <==> del d[key].  O(1)'
        encoded_key = self._encode(key)
        if not self.redis.hdel(self.key, encoded_key):  # Available since Redis 2.0.0
            raise KeyError(key)

    def __iter__(self) -> Generator[JSONTypes, None, None]:
        warnings.warn(
            cast(str, InefficientAccessWarning.__doc__),
            InefficientAccessWarning,
        )
        encoded_items = self.redis.hscan_iter(self.key)  # Available since Redis 2.8.0
        keys = (self._decode(key) for key, _ in encoded_items)
        yield from keys

    def __len__(self) -> int:
        'Return the number of items in the RedisDict.  O(1)'
        return self.redis.hlen(self.key)  # Available since Redis 2.0.0

    # Methods required for Raj's sanity:

    def __repr__(self) -> str:
        'Return the string representation of the RedisDict.  O(n)'
        return f'{self.__class__.__name__}{self.__to_dict()}'

    # Method overrides:

    # From collections.abc.MutableMapping:
    def update(self, arg: InitArg = tuple(), **kwargs: JSONTypes) -> None:  # type: ignore
        with self._watch(arg) as pipeline:
            self.__populate(pipeline, arg, **kwargs)

    # From collections.abc.Mapping:
    def __contains__(self, key: Any) -> bool:
        'd.__contains__(key) <==> key in d.  O(1)'
        try:
            encoded_key = self._encode(key)
        except TypeError:
            return False
        return self.redis.hexists(self.key, encoded_key)  # Available since Redis 2.0.0

    def to_dict(self) -> Dict[JSONTypes, JSONTypes]:
        'Convert a RedisDict into a plain Python dict.'
        warnings.warn(
            cast(str, InefficientAccessWarning.__doc__),
            InefficientAccessWarning,
        )
        encoded_items = self.redis.hgetall(self.key).items()  # Available since Redis 2.0.0
        dict_ = {
            self._decode(encoded_key): self._decode(encoded_value)
            for encoded_key, encoded_value in encoded_items
        }
        return dict_

    __to_dict = to_dict
