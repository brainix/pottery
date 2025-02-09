# --------------------------------------------------------------------------- #
#   test_aioredlock.py                                                        #
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
'Async distributed Redis-powered monotonically increasing ID generator tests.'


import asyncio
import contextlib
import unittest.mock

import pytest
from redis.asyncio import Redis as AIORedis
from redis.commands.core import AsyncScript
from redis.exceptions import TimeoutError

from pottery import AIONextID
from pottery import QuorumNotAchieved


# TODO: When we drop support for Python 3.9, delete the following definition of
# aiter().
try:
    aiter  # type: ignore
except NameError:  # pragma: no cover
    def aiter(iterable):
        return iterable.__aiter__()

# TODO: When we drop support for Python 3.9, delete the following definition of
# anext().
try:
    anext  # type: ignore
except NameError:  # pragma: no cover
    # I got this anext() definition from here:
    #     https://github.com/python/cpython/blob/f4c03484da59049eb62a9bf7777b963e2267d187/Lib/test/test_asyncgen.py#L52
    _NO_DEFAULT = object()

    def anext(iterator, default=_NO_DEFAULT):
        try:
            __anext__ = type(iterator).__anext__
        except AttributeError:
            raise TypeError(f'{iterator!r} is not an async iterator')
        if default is _NO_DEFAULT:
            return __anext__(iterator)

        async def anext_impl():
            try:
                return await __anext__(iterator)
            except StopAsyncIteration:
                return default
        return anext_impl()


@pytest.fixture
def aioids(aioredis: AIORedis) -> AIONextID:  # type: ignore
    return AIONextID(masters={aioredis})


async def test_aionextid(aioids: AIONextID) -> None:
    for expected in range(1, 10):
        got = await anext(aioids)  # type: ignore
        assert got == expected, f'expected {expected}, got {got}'


async def test_reset(aioids: AIONextID) -> None:
    assert await anext(aioids) == 1  # type: ignore
    await aioids.reset()
    assert await anext(aioids) == 1  # type: ignore


@pytest.mark.parametrize('num_aioids', range(1, 6))
async def test_contention(num_aioids: int) -> None:
    dbs = range(1, 6)
    urls = [f'redis://localhost:6379/{db}' for db in dbs]
    masters = [AIORedis.from_url(url, socket_timeout=1) for url in urls]
    aioids = [AIONextID(key='tweet-ids', masters=masters) for _ in range(num_aioids)]

    try:
        coros = [anext(aioids[id_gen]) for id_gen in range(num_aioids)]  # type: ignore
        tasks = [asyncio.create_task(coro) for coro in coros]
        done, _ = await asyncio.wait(tasks)
        results = []
        with contextlib.suppress(QuorumNotAchieved):
            for task in done:
                results.append(task.result())
        assert len(results) == len(set(results))
        # To see the following output, issue:
        # $ source venv/bin/activate; pytest -rP tests/test_aionextid.py::test_contention; deactivate
        print(f'{num_aioids} aioids, {results} IDs')

    finally:
        # Clean up for the next unit test run.
        await aioids[0].reset()


def test_slots(aioids: AIONextID) -> None:
    with pytest.raises(AttributeError):
        aioids.__dict__


def test_aiter(aioids: AIONextID) -> None:
    assert aiter(aioids) is aioids  # type: ignore


async def test_anext_quorumnotachieved(aioids: AIONextID) -> None:
    aioredis = next(iter(aioids.masters))
    with pytest.raises(QuorumNotAchieved), \
         unittest.mock.patch.object(aioredis, 'get') as get:
        get.side_effect = TimeoutError
        await anext(aioids)  # type: ignore

    with pytest.raises(QuorumNotAchieved), \
         unittest.mock.patch.object(AsyncScript, '__call__') as __call__:
        __call__.side_effect = TimeoutError
        await anext(aioids)  # type: ignore


async def test_reset_quorumnotachieved(aioids: AIONextID) -> None:
    aioredis = next(iter(aioids.masters))
    with pytest.raises(QuorumNotAchieved), \
         unittest.mock.patch.object(aioredis, 'delete') as delete:
        delete.side_effect = TimeoutError
        await aioids.reset()


def test_repr(aioids: AIONextID) -> None:
    assert repr(aioids) == '<AIONextID key=nextid:current>'
