# --------------------------------------------------------------------------- #
#   base.py                                                                   #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import doctest
import logging
import random
import sys
import unittest
from typing import NoReturn

from redis import Redis


class TestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logger = logging.getLogger('pottery')
        logger.setLevel(logging.CRITICAL)

    def setUp(self) -> None:
        super().setUp()
        self.redis_db = random.randint(1, 15)
        url = f'redis://localhost:6379/{self.redis_db}'
        self.redis = Redis.from_url(url, socket_timeout=1)
        self.redis.flushdb()
        self.addCleanup(self.redis.flushdb)


def run_doctests() -> NoReturn:  # pragma: no cover
    results = doctest.testmod()
    sys.exit(bool(results.failed))
