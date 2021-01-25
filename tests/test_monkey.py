# --------------------------------------------------------------------------- #
#   test_monkey.py                                                            #
#                                                                             #
#   Copyright © 2015-2021, original authors.                                  #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import json

from tests.base import TestCase


class MonkeyPatchTests(TestCase):
    def test_json_encoder(self):
        try:
            json.dumps(object())
        except TypeError as error:
            assert str(error) in {
                "Object of type 'object' is not JSON serializable",  # Python 3.6
                'Object of type object is not JSON serializable',    # Python 3.7+
            }
