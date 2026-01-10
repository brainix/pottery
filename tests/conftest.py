# --------------------------------------------------------------------------- #
#   conftest.py                                                               #
#                                                                             #
#   Copyright © 2015-2026, Rajiv Bakulesh Shah, original author.              #
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


import random
import sys
import warnings
from typing import AsyncGenerator
from typing import Generator

import pytest
import uvloop
from redis import Redis
from redis.asyncio import Redis as AIORedis

from pottery import PotteryWarning


if sys.version_info < (3, 14):  # pragma: no cover
    @pytest.fixture(scope='session', autouse=True)
    def install_uvloop() -> None:
        uvloop.install()


@pytest.fixture(autouse=True)
def filter_warnings() -> None:
    warnings.filterwarnings('ignore', category=PotteryWarning)


@pytest.fixture(scope='session')
def redis_url() -> str:
    redis_db = random.randint(1, 15)  # nosec
    return f'redis://localhost:6379/{redis_db}'


@pytest.fixture
def redis(redis_url: str) -> Generator[Redis, None, None]:
    redis_client: Redis = Redis.from_url(redis_url, socket_timeout=1)
    redis_client.flushdb()
    yield redis_client
    redis_client.flushdb()


@pytest.fixture
async def aioredis(redis_url: str) -> AsyncGenerator[AIORedis, None]:
    redis_client: AIORedis = AIORedis.from_url(redis_url, socket_timeout=1)
    await redis_client.flushdb()
    yield redis_client
    await redis_client.flushdb()
