# --------------------------------------------------------------------------- #
#   base.py                                                                   #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import doctest
import sys
import unittest
from typing import NoReturn

from pottery.base import Base
from pottery.base import _default_redis


class TestCase(unittest.TestCase):
    _TEST_KEY_PREFIX = 'pottery-test:'

    def setUp(self) -> None:
        super().setUp()
        self.redis = _default_redis

    def tearDown(self) -> None:
        keys_to_delete = []
        for prefix in {Base._RANDOM_KEY_PREFIX, self._TEST_KEY_PREFIX}:
            pattern = prefix + '*'
            keys = self.redis.keys(pattern=pattern)
            keys = (key.decode('utf-8') for key in keys)
            keys_to_delete.extend(keys)
        if keys_to_delete:
            self.redis.delete(*keys_to_delete)
        super().tearDown()


def run_doctests() -> NoReturn:  # pragma: no cover
    results = doctest.testmod()
    sys.exit(bool(results.failed))
