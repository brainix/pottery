# --------------------------------------------------------------------------- #
#   test_redlock_extend_validity.py                                           #
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
'''Tests for Redlock.extend() validity time checks and compensating release.

Per the Redlock specification (https://redis.io/docs/latest/develop/use/patterns/distributed-locks/):
To renew/extend a lock, the client must:
1. Attempt to renew the key with the lock's auto_release_time across all Redis
   masters using an authenticated Lua script (PEXPIRE if value matches).
2. Consider the extension successful only if:
   a. A strict majority (quorum) of masters successfully renewed the lock.
   b. The remaining validity time is strictly positive:
          validity_time = auto_release_time - drift - elapsed_extend_time > 0
3. If quorum is not achieved OR if the validity time has expired, the extension
   is deemed to have failed.  In this failure scenario, the client MUST perform
   a compensating release on all masters holding the token and raise
   ExtendUnlockedLock to prevent leaving stale, orphaned keys.

These tests verify completion-time validity and token-checked compensating
releases for both synchronous Redlock and asynchronous AIORedlock.
'''


import concurrent.futures
import threading
from typing import Awaitable
from typing import Callable
from typing import Iterable
from typing import Literal
from typing import cast

import pytest
from redis import Redis
from redis.asyncio import Redis as AIORedis

import pottery.aioredlock as aioredlock_module
import pottery.redlock as redlock_module
from pottery import AIORedlock
from pottery import ExtendUnlockedLock
from pottery import Redlock


BuildLock = Callable[..., 'tuple[_FakeCluster, Redlock]']
BuildAIOLock = Callable[..., Awaitable['tuple[_FakeCluster, AIORedlock]']]


class _SynchronousExecutor(concurrent.futures.ThreadPoolExecutor):
    '''A BailOutExecutor whose __exit__ waits for every master.

    The real synchronous Redlock returns at quorum and leaves the remaining
    SET/PEXPIRE calls running in background threads (ENV-11).  Waiting for
    them here keeps the replay deterministic; it does not change which calls
    succeed or the quorum outcome.
    '''

    def __exit__(self, *exc_info: object) -> Literal[False]:
        self.shutdown(wait=True)
        return False


class _FakeCluster:
    '''A logically-clocked set of independent in-memory Redis masters.

    `now_ms` is the client's monotonic clock and is only moved explicitly by
    the tests or by the simulated extend latency.  Key expiry is absolute.
    '''

    def __init__(self, num_masters: int, auto_release_time: float) -> None:
        self.now_ms = 0
        self.extend_op_start_ms: int | None = None
        self.extend_latency_ms = 0
        self._lock = threading.Lock()
        self.masters = [_FakeMaster(self) for _ in range(num_masters)]

    def schedule_extend(self, *, start_ms: int, latency_ms: int) -> None:
        self.now_ms = start_ms
        self.extend_op_start_ms = None
        self.extend_latency_ms = latency_ms

    def begin_extend(self) -> int:
        '''Pin the operation start, before any simulated response latency.'''
        with self._lock:
            if self.extend_op_start_ms is None:
                self.extend_op_start_ms = self.now_ms
            return self.extend_op_start_ms

    def end_extend(self) -> None:
        '''The client observes completion only after the slow responses.'''
        with self._lock:
            assert self.extend_op_start_ms is not None
            self.now_ms = self.extend_op_start_ms + self.extend_latency_ms


class _FakeMaster:
    '''Minimal in-memory stand-in for the Redis operations Redlock uses.'''

    def __init__(self, cluster: _FakeCluster) -> None:
        self.cluster = cluster
        self.store: dict[str, tuple[object, int]] = {}

    def _alive(self, name: str, at_ms: int | None = None) -> bool:
        at_ms = self.cluster.now_ms if at_ms is None else at_ms
        entry = self.store.get(name)
        return entry is not None and entry[1] > at_ms

    def get(self, name: str, at_ms: int | None = None) -> object | None:
        if not self._alive(name, at_ms):
            return None
        return self.store[name][0]

    def set(self, name: str, value: object, *, px: int | None = None,
            nx: bool = False, **kwargs: object) -> bool | None:
        if nx and self._alive(name):
            return None
        assert px is not None
        self.store[name] = (value, self.cluster.now_ms + px)
        return True

    def delete(self, name: str) -> int:
        return 1 if self.store.pop(name, None) is not None else 0

    def remaining_ms(self, name: str) -> int:
        if not self._alive(name):
            return 0
        return self.store[name][1] - self.cluster.now_ms


class _FakeScript:
    '''Stand-in for the three Lua scripts registered by Redlock.

    The extend script places the PEXPIRE effect at the operation start (the
    conservative worst case) and only reports completion after the simulated
    latency.
    '''

    def __init__(self, kind: str) -> None:
        self.kind = kind

    def __call__(self, *, keys: tuple[str, ...], args: tuple[object, ...],
                 client: _FakeMaster) -> int:
        name = keys[0]
        token = args[0]
        if self.kind == 'acquired':
            if client.get(name) == token:
                return client.remaining_ms(name)
            return 0
        if self.kind == 'extend':
            cluster = client.cluster
            op_start_ms = cluster.begin_extend()
            extended = 0
            if client.get(name, at_ms=op_start_ms) == token:
                # PEXPIRE at operation start (worst-case request latency).
                ttl_ms = args[1]
                assert isinstance(ttl_ms, int)
                client.store[name] = (token, op_start_ms + ttl_ms)
                extended = 1
            cluster.end_extend()
            return extended
        if self.kind == 'release':
            if client.get(name) == token:
                return client.delete(name)
            return 0
        raise AssertionError(f'unknown fake script kind: {self.kind}')


class _ClusterTimer:
    '''ContextTimer replacement backed by the cluster's logical clock.'''

    def __init__(self, cluster: _FakeCluster) -> None:
        self.cluster = cluster
        self._started: int | None = None
        self._stopped: int | None = None

    def __enter__(self) -> '_ClusterTimer':
        self._started = self.cluster.now_ms
        return self

    def __exit__(self, *exc_info: object) -> Literal[False]:
        self._stopped = self.cluster.now_ms
        return False

    def elapsed(self) -> int:
        assert self._started is not None
        stopped = self.cluster.now_ms if self._stopped is None else self._stopped
        return stopped - self._started


@pytest.fixture
def cluster_and_lock(monkeypatch: pytest.MonkeyPatch) -> BuildLock:
    '''Build a Redlock wired to a fresh in-memory cluster.

    Returns (cluster, lock).  The fake Lua scripts and the logical-clock timer
    are patched only for the duration of the test.
    '''
    def build(*, num_masters: int, auto_release_time: float,
              acquire_at_ms: int = 0) -> tuple[_FakeCluster, Redlock]:
        cluster = _FakeCluster(num_masters, auto_release_time)
        cluster.now_ms = acquire_at_ms
        for attr, kind in (
            ('_acquired_script', 'acquired'),
            ('_extend_script', 'extend'),
            ('_release_script', 'release'),
        ):
            monkeypatch.setattr(
                Redlock,
                attr,
                _FakeScript(kind),
                raising=False,
            )
        monkeypatch.setattr(
            redlock_module,
            'ContextTimer',
            lambda: _ClusterTimer(cluster),
        )
        monkeypatch.setattr(
            redlock_module,
            'BailOutExecutor',
            _SynchronousExecutor,
        )
        lock = Redlock(
            key='printer',
            masters=cast(Iterable[Redis], cluster.masters),
            auto_release_time=auto_release_time,
        )
        assert lock.acquire()
        return cluster, lock
    return build


class _AsyncFakeMaster(_FakeMaster):
    async def set(self, name: str, value: object, *, px: int | None = None,  # type: ignore[override]
                  nx: bool = False, **kwargs: object) -> bool | None:
        return super().set(name, value, px=px, nx=nx, **kwargs)


class _AsyncFakeScript(_FakeScript):
    async def __call__(self, *, keys: tuple[str, ...], args: tuple[object, ...],  # type: ignore[override]
                       client: object) -> int:
        return super().__call__(keys=keys, args=args, client=cast(_FakeMaster, client))


@pytest.fixture
def aio_cluster_and_lock(monkeypatch: pytest.MonkeyPatch) -> BuildAIOLock:
    '''Build an AIORedlock wired to a fresh in-memory cluster.

    Returns an awaitable producing (cluster, lock).
    '''
    async def build(*, num_masters: int, auto_release_time: float,
                    acquire_at_ms: int = 0) -> tuple[_FakeCluster, AIORedlock]:
        cluster = _FakeCluster(num_masters, auto_release_time)
        cluster.masters = [_AsyncFakeMaster(cluster) for _ in range(num_masters)]
        cluster.now_ms = acquire_at_ms
        for attr, kind in (
            ('_acquired_script', 'acquired'),
            ('_extend_script', 'extend'),
            ('_release_script', 'release'),
        ):
            monkeypatch.setattr(
                AIORedlock,
                attr,
                _AsyncFakeScript(kind),
                raising=False,
            )
        monkeypatch.setattr(
            aioredlock_module,
            'ContextTimer',
            lambda: _ClusterTimer(cluster),
        )
        lock = AIORedlock(
            key='printer',
            masters=cast(Iterable[AIORedis], cluster.masters),
            auto_release_time=auto_release_time,
        )
        assert await lock.acquire()
        return cluster, lock
    return build


# --------------------------------------------------------------------------- #
#   Extend timeout / validity expiration scenario                             #
#   n=5, ART=200 ms, drift=4 ms, quorum=3.                                    #
#   Even if a majority renews the token, the extension fails because the      #
#   elapsed time exceeds the lock validity window.                            #
# --------------------------------------------------------------------------- #

def test_fixed_extend_success_without_validity(cluster_and_lock: BuildLock) -> None:
    cluster, lock = cluster_and_lock(num_masters=5, auto_release_time=.2, acquire_at_ms=940)
    cluster.schedule_extend(start_ms=1133, latency_ms=205)

    with pytest.raises(ExtendUnlockedLock):
        lock.extend()

    assert lock._extension_num == 0
    assert all(master.get(lock.key) is None for master in cluster.masters)


def test_fixed_extend_fast_success(cluster_and_lock: BuildLock) -> None:
    cluster, lock = cluster_and_lock(num_masters=5, auto_release_time=.2, acquire_at_ms=940)
    cluster.schedule_extend(start_ms=950, latency_ms=10)

    lock.extend()

    assert lock._extension_num == 1
    assert lock.locked() > 0
    assert all(master.get(lock.key) == lock._uuid for master in cluster.masters)


# --------------------------------------------------------------------------- #
#   Even-sized cluster validity expiration scenario                           #
#   n=4 (even), ART=200 ms, drift=4 ms, quorum=3.                             #
#   Even with a strict majority (3/4) renewing the token, the extension fails #
#   when the completion instant exhausts the validity deadline.               #
# --------------------------------------------------------------------------- #

def test_fixed_even_extend_success_without_validity(cluster_and_lock: BuildLock) -> None:
    cluster, lock = cluster_and_lock(num_masters=4, auto_release_time=.2, acquire_at_ms=738)
    cluster.schedule_extend(start_ms=900, latency_ms=200)

    with pytest.raises(ExtendUnlockedLock):
        lock.extend()

    assert lock._extension_num == 0
    assert all(master.get(lock.key) is None for master in cluster.masters)


# --------------------------------------------------------------------------- #
#   Slow extend validity expiration scenario (1-second scale)                 #
#   n=5, ART=1000 ms, drift=12 ms, quorum=3.                                  #
#   Verifies that validity check correctly fails at longer TTL durations when #
#   the elapsed time exhausts remaining validity.                             #
# --------------------------------------------------------------------------- #

def test_fixed_slow_extend_success_without_validity(cluster_and_lock: BuildLock) -> None:
    cluster, lock = cluster_and_lock(num_masters=5, auto_release_time=1, acquire_at_ms=5043)
    cluster.schedule_extend(start_ms=5698, latency_ms=994)

    with pytest.raises(ExtendUnlockedLock):
        lock.extend()

    assert lock._extension_num == 0
    assert all(master.get(lock.key) is None for master in cluster.masters)


# --------------------------------------------------------------------------- #
#   Compensating release on late extend failure                               #
#   n=5, ART=200 ms, drift=4 ms, quorum=3.                                    #
#   When extend completes too late, any renewed keys must be compensated via   #
#   token-checked release, leaving no orphaned keys across masters.           #
# --------------------------------------------------------------------------- #

def test_fixed_late_extend_no_compensation(cluster_and_lock: BuildLock) -> None:
    cluster, lock = cluster_and_lock(num_masters=5, auto_release_time=.2, acquire_at_ms=239)
    cluster.schedule_extend(start_ms=267, latency_ms=198)

    with pytest.raises(ExtendUnlockedLock):
        lock.extend()

    assert lock._extension_num == 0
    assert all(master.get(lock.key) is None for master in cluster.masters)


# --------------------------------------------------------------------------- #
#   Async extend validity expiration scenario                                 #
# --------------------------------------------------------------------------- #

async def test_fixed_aio_extend_success_without_validity(aio_cluster_and_lock: BuildAIOLock) -> None:
    cluster, lock = await aio_cluster_and_lock(num_masters=5, auto_release_time=.2, acquire_at_ms=940)
    cluster.schedule_extend(start_ms=1133, latency_ms=205)

    with pytest.raises(ExtendUnlockedLock):
        await lock.extend()

    assert lock._extension_num == 0
    assert all(master.get(lock.key) is None for master in cluster.masters)


async def test_fixed_aio_extend_fast_success(aio_cluster_and_lock: BuildAIOLock) -> None:
    cluster, lock = await aio_cluster_and_lock(num_masters=5, auto_release_time=.2, acquire_at_ms=940)
    cluster.schedule_extend(start_ms=950, latency_ms=10)

    await lock.extend()

    assert lock._extension_num == 1
    assert await lock.locked() > 0
    assert all(master.get(lock.key) == lock._uuid for master in cluster.masters)


# --------------------------------------------------------------------------- #
#   Async compensating release on late extend failure                         #
# --------------------------------------------------------------------------- #

async def test_fixed_aio_late_extend_no_compensation(aio_cluster_and_lock: BuildAIOLock) -> None:
    cluster, lock = await aio_cluster_and_lock(num_masters=5, auto_release_time=.2, acquire_at_ms=239)
    cluster.schedule_extend(start_ms=267, latency_ms=198)

    with pytest.raises(ExtendUnlockedLock):
        await lock.extend()

    assert lock._extension_num == 0
    assert all(master.get(lock.key) is None for master in cluster.masters)
