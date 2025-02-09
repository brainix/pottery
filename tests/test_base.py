# --------------------------------------------------------------------------- #
#   test_base.py                                                              #
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


import gc
import unittest.mock
from typing import Generator

import pytest
from redis import Redis

from pottery import RandomKeyError
from pottery import RedisDict
from pottery.base import Iterable_
from pottery.base import Primitive
from pottery.base import _Comparable
from pottery.base import _Pipelined
from pottery.base import random_key


class TestRandomKey:
    @staticmethod
    def test_random_key_raises_typeerror_for_invalid_num_tries(redis: Redis) -> None:
        with pytest.raises(TypeError):
            random_key(redis=redis, num_tries=3.0)  # type: ignore

    @staticmethod
    def test_random_key_raises_valueerror_for_invalid_num_tries(redis: Redis) -> None:
        with pytest.raises(ValueError):
            random_key(redis=redis, num_tries=-1)

    @staticmethod
    def test_random_key_raises_randomkeyerror_when_no_tries_left(redis: Redis) -> None:
        with pytest.raises(RandomKeyError), \
             unittest.mock.patch.object(redis, 'exists') as exists:
            exists.return_value = True
            random_key(redis=redis)


class TestCommon:
    @staticmethod
    def test_out_of_scope(redis: Redis) -> None:
        def scope() -> str:
            raj = RedisDict(redis=redis, hobby='music', vegetarian=True)
            assert redis.exists(raj.key)
            return raj.key

        key = scope()
        gc.collect()
        assert not redis.exists(key)

    @staticmethod
    def test_del(redis: Redis) -> None:
        raj = RedisDict(redis=redis, key='pottery:raj', hobby='music', vegetarian=True)
        nilika = RedisDict(redis=redis, key='pottery:nilika', hobby='music', vegetarian=True)
        luvh = RedisDict(redis=redis, key='luvh', hobby='bullying', vegetarian=False)

        with unittest.mock.patch.object(redis, 'unlink') as unlink:
            del raj
            gc.collect()
            unlink.assert_called_with('pottery:raj')
            unlink.reset_mock()

            del nilika
            unlink.assert_called_with('pottery:nilika')
            unlink.reset_mock()

            del luvh
            unlink.assert_not_called()

    @staticmethod
    def test_eq(redis: Redis) -> None:
        raj = RedisDict(redis=redis, key='pottery:raj', hobby='music', vegetarian=True)
        nilika = RedisDict(redis=redis, key='pottery:nilika', hobby='music', vegetarian=True)
        luvh = RedisDict(redis=redis, key='luvh', hobby='bullying', vegetarian=False)

        assert raj == raj
        assert raj == nilika
        assert raj == {'hobby': 'music', 'vegetarian': True}
        assert not raj == luvh
        assert not raj == None

    @staticmethod
    def test_ne(redis: Redis) -> None:
        raj = RedisDict(redis=redis, key='pottery:raj', hobby='music', vegetarian=True)
        nilika = RedisDict(redis=redis, key='pottery:nilika', hobby='music', vegetarian=True)
        luvh = RedisDict(redis=redis, key='luvh', hobby='bullying', vegetarian=False)

        assert not raj != raj
        assert not raj != nilika
        assert not raj != {'hobby': 'music', 'vegetarian': True}
        assert raj != luvh
        assert raj != None

    @staticmethod
    def test_randomkeyerror_raised(redis: Redis) -> None:
        raj = RedisDict(redis=redis, key='pottery:raj', hobby='music', vegetarian=True)

        with pytest.raises(RandomKeyError), \
             unittest.mock.patch.object(raj.redis, 'exists') as exists:
            exists.return_value = True
            raj._random_key()

    @staticmethod
    def test_randomkeyerror_repr(redis: Redis) -> None:
        raj = RedisDict(redis=redis, key='pottery:raj', hobby='music', vegetarian=True)

        with unittest.mock.patch.object(raj.redis, 'exists') as exists:
            exists.return_value = True
            try:
                raj._random_key()
            except RandomKeyError as wtf:
                redis_db = redis.get_connection_kwargs()['db']  # type: ignore
                assert repr(wtf) == (
                    f'RandomKeyError(redis=<redis.client.Redis(<redis.connection.ConnectionPool(<redis.connection.Connection(host=localhost,port=6379,db={redis_db})>)>)>, key=None)'
                )
            else:  # pragma: no cover
                pytest.fail(reason='RandomKeyError not raised')


class TestEncodable:
    @staticmethod
    @pytest.fixture
    def decoded_redis(redis_url: str) -> Generator[Redis, None, None]:
        redis = Redis.from_url(redis_url, socket_timeout=1, decode_responses=True)
        redis.flushdb()
        yield redis
        redis.flushdb()

    @staticmethod
    def test_decoded_responses(decoded_redis: Redis) -> None:
        'Ensure that Pottery still works if the Redis client decodes responses.'
        tel = RedisDict({'jack': 4098, 'sape': 4139}, redis=decoded_redis)  # type: ignore

        # Ensure that repr(tel) does not raise this exception:
        #
        # Traceback (most recent call last):
        #   File "/Users/rajiv.shah/Documents/Code/pottery/tests/test_base.py", line 139, in test_decoded_responses
        #     repr(tel)
        #   File "/Users/rajiv.shah/Documents/Code/pottery/pottery/dict.py", line 116, in __repr__
        #     dict_ = {self._decode(key): self._decode(value) for key, value in items}
        #   File "/Users/rajiv.shah/Documents/Code/pottery/pottery/dict.py", line 116, in <dictcomp>
        #     dict_ = {self._decode(key): self._decode(value) for key, value in items}
        #   File "/Users/rajiv.shah/Documents/Code/pottery/pottery/base.py", line 154, in _decode
        #     decoded: JSONTypes = json.loads(value.decode())
        # AttributeError: 'str' object has no attribute 'decode'
        repr(tel)


class TestPipelined:
    @staticmethod
    def test_abc_cant_be_instantiated():
        with pytest.raises(TypeError):
            _Pipelined()


class TestComparable:
    @staticmethod
    def test_abc_cant_be_instantiated() -> None:
        with pytest.raises(TypeError):
            _Comparable()  # type: ignore


class TestIterable:
    @staticmethod
    def test_abc_cant_be_instantiated() -> None:
        with pytest.raises(TypeError):
            Iterable_()  # type: ignore

    @staticmethod
    def test_iter(redis: Redis) -> None:
        garbage = RedisDict(redis=redis)
        for num in range(1024):
            garbage[num] = num
        assert set(iter(garbage)) == set(range(1024))


class TestPrimitive:
    @staticmethod
    def test_abc_cant_be_instantiated() -> None:
        with pytest.raises(TypeError):
            Primitive(key='abc')  # type: ignore
