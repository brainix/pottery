# --------------------------------------------------------------------------- #
#   test_monkey.py                                                            #
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


import json

import pytest
from redis import Redis

from pottery import RedisDict
from pottery import RedisList


def test_typeerror_not_jsonifyable() -> None:
    "Ensure json.dumps() raises TypeError for objs that can't be serialized"
    try:
        json.dumps(object())
    except TypeError as error:
        assert str(error) == 'Object of type object is not JSON serializable'


def test_dict() -> None:
    'Ensure that json.dumps() can serialize a dict'
    assert json.dumps({}) == '{}'


def test_redisdict(redis: Redis) -> None:
    'Ensure that json.dumps() can serialize a RedisDict'
    dict_ = RedisDict(redis=redis)
    assert json.dumps(dict_) == '{}'


def test_list() -> None:
    'Ensure that json.dumps() can serialize a list'
    assert json.dumps([]) == '[]'


def test_redislist(redis: Redis) -> None:
    'Ensure that json.dumps() can serialize a RedisList'
    list_ = RedisList(redis=redis)
    assert json.dumps(list_) == '[]'


def test_json_encoder(redis: Redis) -> None:
    'Ensure that we can pass in the cls keyword argument to json.dumps()'
    dict_ = RedisDict(redis=redis)
    with pytest.raises(TypeError):
        json.dumps(dict_, cls=None)
