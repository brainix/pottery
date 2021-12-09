# --------------------------------------------------------------------------- #
#   list.py                                                                   #
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
import functools
import itertools
import uuid
import warnings
from typing import Any
from typing import Callable
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union
from typing import cast

from redis import Redis
from redis import ResponseError
from redis.client import Pipeline

from .annotations import F
from .base import Base
from .base import JSONTypes
from .exceptions import InefficientAccessWarning
from .exceptions import KeyExistsError


def _raise_on_error(func: F) -> Callable[[F], F]:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except ResponseError as error:
            raise IndexError('list assignment index out of range') from error
    return wrapper


class RedisList(Base, collections.abc.MutableSequence):
    'Redis-backed container compatible with Python lists.'

    def __slice_to_indices(self, slice_or_index: Union[slice, int]) -> range:
        if isinstance(slice_or_index, slice):
            start, stop, step = cast(slice, slice_or_index).indices(len(self))
        elif isinstance(slice_or_index, int):
            start = cast(int, slice_or_index)
            stop = cast(int, slice_or_index) + 1
            step = 1
        else:
            raise TypeError(
                'list indices must be integers or slices, '
                f'not {slice_or_index.__class__.__name__}'
            )
        indices = range(start, stop, step)
        return indices

    def __init__(self,
                 iterable: Iterable[JSONTypes] = tuple(),
                 *,
                 redis: Optional[Redis] = None,
                 key: Optional[str] = None,
                 ) -> None:
        'Initialize the RedisList.  O(n)'
        super().__init__(redis=redis, key=key)
        if iterable:
            with self._watch(iterable) as pipeline:
                if pipeline.exists(self.key):
                    raise KeyExistsError(pipeline, self.key)
                self._populate(pipeline, iterable)

    def _populate(self,
                  pipeline: Pipeline,
                  iterable: Iterable[JSONTypes] = tuple(),
                  ) -> None:
        encoded_values = [self._encode(value) for value in iterable]
        if encoded_values:
            pipeline.multi()
            pipeline.rpush(self.key, *encoded_values)

    # Methods required by collections.abc.MutableSequence:

    def __getitem__(self, index: Union[slice, int]) -> Any:
        'l.__getitem__(index) <==> l[index].  O(n)'
        with self._watch() as pipeline:
            if isinstance(index, slice):
                # Python's list API requires us to get elements by slice (a
                # start index, a stop index, and a step).  Redis supports only
                # getting elements by start and stop (no step).  So our
                # workaround is to get all of the elements between start and
                # stop from Redis, then discard the ones between step in Python
                # code.  More info:
                #   http://redis.io/commands/lrange
                warnings.warn(
                    cast(str, InefficientAccessWarning.__doc__),
                    InefficientAccessWarning,
                )
                indices = self.__slice_to_indices(index)
                if indices.step >= 0:
                    start, stop = indices.start, indices.stop-1
                else:
                    start, stop = indices.stop+1, indices.start
                pipeline.multi()
                pipeline.lrange(self.key, start, stop)
                encoded = pipeline.execute()[0]
                encoded = encoded[::index.step]
                value: Union[List[JSONTypes], JSONTypes] = [
                    self._decode(value) for value in encoded
                ]
            else:
                index = self.__slice_to_indices(index).start
                len_ = cast(int, pipeline.llen(self.key))
                if index not in {-1, 0, len_-1}:
                    warnings.warn(
                        cast(str, InefficientAccessWarning.__doc__),
                        InefficientAccessWarning,
                    )
                encoded = pipeline.lindex(self.key, index)
                if encoded is None:
                    raise IndexError('list index out of range')
                value = self._decode(encoded)
        return value

    @_raise_on_error
    def __setitem__(self, index: int, value: JSONTypes) -> None:  # type: ignore
        'l.__setitem__(index, value) <==> l[index] = value.  O(n)'
        with self._watch() as pipeline:
            if isinstance(index, slice):
                warnings.warn(
                    cast(str, InefficientAccessWarning.__doc__),
                    InefficientAccessWarning,
                )
                encoded_values = [self._encode(value) for value in value]
                indices = self.__slice_to_indices(index)
                pipeline.multi()
                for index, encoded_value in zip(indices, encoded_values):
                    pipeline.lset(self.key, index, encoded_value)
                indices, num = indices[len(encoded_values):], 0
                for index in indices:
                    pipeline.lset(self.key, index, 0)
                    num += 1
                if num:
                    pipeline.lrem(self.key, num, 0)
            else:
                index = self.__slice_to_indices(index).start
                len_ = cast(int, pipeline.llen(self.key))
                if index not in {-1, 0, len_-1}:
                    warnings.warn(
                        cast(str, InefficientAccessWarning.__doc__),
                        InefficientAccessWarning,
                    )
                pipeline.multi()
                pipeline.lset(self.key, index, self._encode(value))

    @_raise_on_error
    def __delitem__(self, index: Union[slice, int]) -> None:  # type: ignore
        'l.__delitem__(index) <==> del l[index].  O(n)'
        with self._watch() as pipeline:
            self.__delete(pipeline, index)

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    def __delete(self, pipeline: Pipeline, index: Union[slice, int]) -> None:
        # Python's list API requires us to delete an element by *index.*  Redis
        # supports only deleting an element by *value.*  So our workaround is
        # to set l[index] to a UUID4, then to delete the value UUID4.  More
        # info:
        #   http://redis.io/commands/lrem
        warnings.warn(
            cast(str, InefficientAccessWarning.__doc__),
            InefficientAccessWarning,
        )
        indices, num = self.__slice_to_indices(index), 0
        uuid4 = str(uuid.uuid4())
        pipeline.multi()
        for index in indices:
            pipeline.lset(self.key, index, uuid4)
            num += 1
        if num:
            pipeline.lrem(self.key, num, uuid4)

    def __len__(self) -> int:
        'Return the number of items in the RedisList.  O(1)'
        return self.redis.llen(self.key)

    def insert(self, index: int, value: JSONTypes) -> None:
        'Insert an element into the RedisDeque before the given index.  O(n)'
        with self._watch() as pipeline:
            self.__insert(index, value, pipeline=pipeline)

    def _insert(self,
                index: int,
                value: JSONTypes,
                *,
                pipeline: Pipeline,
                ) -> None:
        encoded_value = self._encode(value)
        current_length = cast(int, pipeline.llen(self.key))
        if 0 < index < current_length:
            # Python's list API requires us to insert an element before the
            # given *index.*  Redis supports only inserting an element before a
            # given (pivot) *value.*  So our workaround is to set the pivot
            # value to a UUID4, then to insert the desired value before the
            # UUID4, then to set the value UUID4 back to the original pivot
            # pivot value.  More info:
            #   http://redis.io/commands/linsert
            warnings.warn(
                cast(str, InefficientAccessWarning.__doc__),
                InefficientAccessWarning,
            )
            uuid4 = str(uuid.uuid4())
            pivot = cast(bytes, pipeline.lindex(self.key, index))
            pipeline.multi()
            pipeline.lset(self.key, index, uuid4)
            pipeline.linsert(self.key, 'BEFORE', uuid4, encoded_value)
            pipeline.lset(self.key, index+1, pivot)
        else:
            pipeline.multi()
            push_method = pipeline.lpush if index <= 0 else pipeline.rpush
            push_method(self.key, encoded_value)

    __insert = _insert

    # Methods required for Raj's sanity:

    def sort(self, *, key: Optional[str] = None, reverse: bool = False) -> None:
        'Sort the RedisList in place.  O(n)'
        if key is not None:
            raise NotImplementedError('sorting by key not implemented')
        warnings.warn(
            cast(str, InefficientAccessWarning.__doc__),
            InefficientAccessWarning,
        )
        self.redis.sort(self.key, desc=reverse, store=self.key)

    def __eq__(self, other: Any) -> bool:
        if self is other:
            return True

        if self._same_redis(other) and self.key == other.key:
            # self and other are both RedisLists on the same Redis database and
            # with the same key.  No need to compare element by element.
            return True

        with self._watch(other) as pipeline:
            len_xs = cast(int, pipeline.llen(self.key))
            if isinstance(other, RedisList):
                len_ys = cast(int, pipeline.llen(other.key))
            else:
                try:
                    len_ys = len(other)
                except TypeError:
                    # TypeError: other has no len()
                    return False
            if len_xs != len_ys:
                # self and other are different lengths.
                return False

            if isinstance(other, collections.abc.MutableSequence):
                # self and other are the same length, and other is a mutable
                # sequence too.  Compare self's and other's elements, pair by
                # pair.
                warnings.warn(
                    cast(str, InefficientAccessWarning.__doc__),
                    InefficientAccessWarning,
                )
                encoded_xs = cast(
                    List[bytes],
                    pipeline.lrange(self.key, 0, -1),
                )
                decoded_xs = (self._decode(x) for x in encoded_xs)
                if isinstance(other, RedisList):
                    encoded_ys = cast(
                        List[bytes],
                        pipeline.lrange(other.key, 0, -1),
                    )
                    decoded_ys: collections.abc.Iterable[Any] = (  # pragma: no cover
                        self._decode(y) for y in encoded_ys
                    )
                else:
                    decoded_ys = other
                return all(x == y for x, y in zip(decoded_xs, decoded_ys))

        # self and other are the same length, but other is an unordered
        # collection.
        return False

    def __add__(self, other: List[JSONTypes]) -> 'RedisList':
        'Append the items in other to the RedisList.  O(n)'
        warnings.warn(
            cast(str, InefficientAccessWarning.__doc__),
            InefficientAccessWarning,
        )
        with self._watch(other):
            iterable = itertools.chain(self, other)
            return self.__class__(iterable, redis=self.redis)

    def __repr__(self) -> str:
        'Return the string representation of the RedisList.  O(n)'
        warnings.warn(
            cast(str, InefficientAccessWarning.__doc__),
            InefficientAccessWarning,
        )
        return self.__class__.__name__ + str(self.to_list())

    # Method overrides:

    # From collections.abc.MutableSequence:
    def append(self, value: JSONTypes) -> None:
        'Add an element to the right side of the RedisList.  O(1)'
        self.__extend((value,))

    # From collections.abc.MutableSequence:
    def extend(self, values: Iterable[JSONTypes]) -> None:
        'Extend the RedisList by appending elements from the iterable.  O(1)'
        encoded_values = (self._encode(value) for value in values)
        self.redis.rpush(self.key, *encoded_values)

    __extend = extend

    # From collections.abc.MutableSequence:
    def pop(self, index: Optional[int] = None) -> JSONTypes:
        with self._watch() as pipeline:
            len_ = len(self)
            if index and index >= len_:
                raise IndexError('pop index out of range')
            elif index in {0, None, len_-1, -1}:
                pop_method = 'lpop' if index == 0 else 'rpop'
                pipeline.multi()
                getattr(pipeline, pop_method)(self.key)
                encoded_value = pipeline.execute()[0]
                if encoded_value is None:
                    raise IndexError(
                        f'pop from an empty {self.__class__.__name__}'
                    )
                return self._decode(encoded_value)
            else:
                value: JSONTypes = self[cast(int, index)]
                self.__delete(pipeline, cast(int, index))
                return value

    # From collections.abc.MutableSequence:
    def remove(self, value: JSONTypes) -> None:
        'Remove first occurrence of value.  O(n)'
        warnings.warn(
            cast(str, InefficientAccessWarning.__doc__),
            InefficientAccessWarning,
        )
        with self._watch() as pipeline:
            for index, element in enumerate(self):
                if element == value:
                    self.__delete(pipeline, index)
                    return
        class_name = self.__class__.__name__
        raise ValueError(f'{class_name}.remove(x): x not in {class_name}')

    def to_list(self) -> List[JSONTypes]:
        'Convert the RedisList to a Python list.  O(n)'
        warnings.warn(
            cast(str, InefficientAccessWarning.__doc__),
            InefficientAccessWarning,
        )
        encoded = self.redis.lrange(self.key, 0, -1)
        values = [self._decode(value) for value in encoded]
        return values
