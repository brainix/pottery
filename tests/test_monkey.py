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

from tests.base import TestCase


class Incorrect:
    '''This class defines a .to_dict() and a .to_list() method.

    This means that our monkey patch doesn't know how to JSON serialize
    Incorrect objects.
    '''

    def to_dict(self):  # pragma: no cover
        return {}

    def to_list(self):  # pragma: no cover
        return []


class Correct:
    '''This class defines a .to_dict() method.

    This means that our monkey patch knows how to JSON serialize Correct
    objects.
    '''

    def to_dict(self):
        return {}


class MonkeyPatchTests(TestCase):
    def test_typeerror_not_jsonifyable(self):
        "Ensure json.dumps() raises TypeError for objs that can't be serialized"
        try:
            json.dumps(object())
        except TypeError as error:
            assert str(error) == 'Object of type object is not JSON serializable'

    def test_typeerror_multiple_methods(self):
        "Ensure json.dumps() raises TypeError for objs with multiple .to_* methods"
        try:
            json.dumps(Incorrect())
        except TypeError as error:
            assert str(error) == (
                "Incorrect.to_dict(), Incorrect.to_list() defined; "
                "don't know how to JSONify Incorrect objects"
            )

    def test_dict(self):
        "Ensure that json.dumps() can serialize a dict"
        assert json.dumps({}) == '{}'

    def test_to_dict(self):
        "Ensure that json.dumps() can serialize an obj with a .to_dict() method"
        assert json.dumps(Correct()) == '{}'
