# --------------------------------------------------------------------------- #
#   test_nextid.py                                                            #
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
'Distributed Redis-powered monotonically increasing ID generator tests.'


import concurrent.futures
import contextlib
import unittest.mock
from typing import Generator

import pytest
from redis import Redis
from redis.commands.core import Script
from redis.exceptions import TimeoutError

from pottery import NextID
from pottery import QuorumIsImpossible
from pottery import QuorumNotAchieved


@pytest.fixture
def ids(redis: Redis) -> Generator[NextID, None, None]:
    redis.unlink('nextid:current')
    yield NextID(masters={redis})
    redis.unlink('nextid:current')


def test_nextid(ids: NextID) -> None:
    for id_ in range(1, 10):
        assert next(ids) == id_


def test_iter(ids: NextID) -> None:
    assert iter(ids) is ids


def test_reset(ids: NextID) -> None:
    ids.reset()
    for redis in ids.masters:
        assert not redis.exists(ids.key)

    assert next(ids) == 1
    for redis in ids.masters:
        assert redis.exists(ids.key)

    ids.reset()
    for redis in ids.masters:
        assert not redis.exists(ids.key)

    assert next(ids) == 1
    for redis in ids.masters:
        assert redis.exists(ids.key)


@pytest.mark.parametrize('num_ids', range(1, 6))
def test_contention(num_ids: int) -> None:
    dbs = range(1, 6)
    urls = [f'redis://localhost:6379/{db}' for db in dbs]
    masters = [Redis.from_url(url, socket_timeout=1) for url in urls]
    ids = [NextID(key='tweet-ids', masters=masters) for _ in range(num_ids)]

    try:
        results = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(next, i) for i in ids]  # type: ignore
            for future in concurrent.futures.as_completed(futures):
                with contextlib.suppress(QuorumNotAchieved):
                    result = future.result()
                    results.append(result)
        assert len(results) == len(set(results))
        # To see the following output, issue:
        # $ source venv/bin/activate; pytest -rP tests/test_nextid.py::test_contention; deactivate
        print(f'{num_ids} ids, {results} IDs')

    finally:
        ids[0].reset()


def test_repr(ids: NextID) -> None:
    assert repr(ids) == '<NextID key=nextid:current>'


def test_slots(ids: NextID) -> None:
    with pytest.raises(AttributeError):
        ids.__dict__


def test_next_quorumnotachieved(ids: NextID) -> None:
    with pytest.raises(QuorumNotAchieved), \
         unittest.mock.patch.object(next(iter(ids.masters)), 'get') as get:
        get.side_effect = TimeoutError
        next(ids)

    with pytest.raises(QuorumNotAchieved), \
         unittest.mock.patch.object(Script, '__call__') as __call__:
        __call__.side_effect = TimeoutError
        next(ids)


def test_next_quorumisimpossible(redis: Redis) -> None:
    ids = NextID(masters={redis}, raise_on_redis_errors=True)
    with pytest.raises(QuorumIsImpossible), \
         unittest.mock.patch.object(next(iter(ids.masters)), 'get') as get:
        get.side_effect = TimeoutError
        next(ids)

    with pytest.raises(QuorumIsImpossible), \
         unittest.mock.patch.object(Script, '__call__') as __call__:
        __call__.side_effect = TimeoutError
        next(ids)


def test_reset_quorumnotachieved(ids: NextID) -> None:
    with pytest.raises(QuorumNotAchieved), \
         unittest.mock.patch.object(next(iter(ids.masters)), 'delete') as delete:
        delete.side_effect = TimeoutError
        ids.reset()


def test_reset_quorumisimpossible(redis: Redis) -> None:
    ids = NextID(masters={redis}, raise_on_redis_errors=True)
    with pytest.raises(QuorumIsImpossible), \
         unittest.mock.patch.object(next(iter(ids.masters)), 'delete') as delete:
        delete.side_effect = TimeoutError
        ids.reset()
