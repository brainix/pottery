# --------------------------------------------------------------------------- #
#   test_monkey.py                                                            #
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


import json

from pottery import RedisDict
from pottery import RedisList
from tests.base import TestCase


class MonkeyPatchTests(TestCase):
    def test_typeerror_not_jsonifyable(self):
        "Ensure json.dumps() raises TypeError for objs that can't be serialized"
        try:
            json.dumps(object())
        except TypeError as error:
            assert str(error) == 'Object of type object is not JSON serializable'

    def test_dict(self):
        "Ensure that json.dumps() can serialize a dict"
        assert json.dumps({}) == '{}'

    def test_redisdict(self):
        "Ensure that json.dumps() can serialize a RedisDict"
        dict_ = RedisDict(redis=self.redis)
        assert json.dumps(dict_) == '{}'

    def test_list(self):
        "Ensure that json.dumps() can serialize a list"
        assert json.dumps([]) == '[]'

    def test_redislist(self):
        "Ensure that json.dumps() can serialize a RedisList"
        list_ = RedisList(redis=self.redis)
        assert json.dumps(list_) == '[]'
