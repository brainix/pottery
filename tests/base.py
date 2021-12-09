# --------------------------------------------------------------------------- #
#   base.py                                                                   #
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


import doctest
import logging
import random
import sys
import unittest
import warnings
from typing import NoReturn

from redis import Redis

from pottery import InefficientAccessWarning


class TestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logger = logging.getLogger('pottery')
        logger.setLevel(logging.CRITICAL)
        warnings.filterwarnings('ignore', category=InefficientAccessWarning)

    def setUp(self) -> None:
        super().setUp()

        # Choose a random Redis database for this test.
        self.redis_db = random.randint(1, 15)
        url = f'redis://localhost:6379/{self.redis_db}'

        # Set up our Redis clients.
        self.redis = Redis.from_url(url, socket_timeout=1)
        self.redis_decoded_responses = Redis.from_url(
            url,
            socket_timeout=1,
            decode_responses=True,
        )

        # Clean up the Redis database before and after the test.
        self.redis.flushdb()
        self.addCleanup(self.redis.flushdb)


def run_doctests() -> NoReturn:  # pragma: no cover
    results = doctest.testmod()
    sys.exit(bool(results.failed))
