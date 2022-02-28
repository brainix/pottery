# --------------------------------------------------------------------------- #
#   base.py                                                                   #
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


import asyncio
import doctest
import functools
import logging
import random
import sys
import unittest
import warnings
from typing import Any
from typing import NoReturn
from typing import cast

from redis import Redis
from redis.asyncio import Redis as AIORedis  # type: ignore

from pottery import PotteryWarning
from pottery.annotations import F
from pottery.base import logger


class TestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logger.setLevel(logging.CRITICAL)
        warnings.filterwarnings('ignore', category=PotteryWarning)

    def setUp(self) -> None:
        super().setUp()

        # Choose a random Redis database for this test.
        self.redis_db = random.randint(1, 15)  # nosec
        url = f'redis://localhost:6379/{self.redis_db}'

        # Set up our Redis clients.
        self.redis = Redis.from_url(url, socket_timeout=1)
        self.redis_decoded_responses = Redis.from_url(
            url,
            socket_timeout=1,
            decode_responses=True,
        )
        self.aioredis = AIORedis.from_url(url, socket_timeout=1)

        # Clean up the Redis database before and after the test.
        self.redis.flushdb()
        self.addCleanup(self.redis.flushdb)


def async_test(coro: F) -> F:
    '''Decorator for async unit tests.

    I got this recipe from:
        https://stackoverflow.com/a/46324983
    '''
    @functools.wraps(coro)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        event_loop = asyncio.new_event_loop()
        scheduled = coro(*args, **kwargs)
        try:
            return event_loop.run_until_complete(scheduled)
        finally:
            event_loop.close()
    return cast(F, wrapper)


def run_doctests() -> NoReturn:  # pragma: no cover
    results = doctest.testmod()
    sys.exit(bool(results.failed))
