
## What this project is
- Pottery: a small library of Redis-backed Python containers and distributed primitives (dict, set, list, deque, counter, redlock, nextid, bloom, hyper, cache, timers, etc.). See `README.md` for usage examples.
- Key design points: JSON-encoding for values (`_Encodable` in `pottery/base.py`), careful use of Redis pipelines and WATCH (`_Pipelined`), and both synchronous and asyncio variants (e.g., `nextid.py` / `aionextid.py`, `redlock.py` / `aioredlock.py`).

## Quick dev & CI workflow (how to run things locally)
- Tests require a running Redis instance at `localhost:6379` (GitHub Actions uses a Redis service). Locally you can run:
  - Docker: `docker run --rm -p 6379:6379 redis`
  - macOS/Homebrew: `brew services start redis` (Makefile has convenience targets too).
- Run tests: `pytest --verbose` or `make test` (the `make test` target also runs mypy, flake8, isort check, bandit and safety in a venv).
- CI: `.github/workflows/python-package.yml` runs pytest, mypy, flake8/isort, bandit, safety across supported Python versions.

## Important patterns & conventions (do not change without cause)
- Constructor convention for containers: `RedisX(..., redis=redis_client, key='name')` — the **first keyword is `redis`, second is `key`**. Many README examples follow this order.
- Values and elements must be JSON-serializable. Encoding/decoding is centralized in `pottery/base.py` (`_Encodable._encode/_decode`). Prefer that helper for consistency.
- Use the `_watch(...)` context manager for multi-key/atomic operations to get correct WATCH/pipeline semantics (defined in `pottery/base.py` via `_Pipelined`).
- Equality and comparison optimizations: `Container` subclasses warn about inefficient accesses and sometimes fall back to pipeline-based comparisons — see `_Comparable` in `base.py`.
- Async parity: new sync primitives generally get an asyncio counterpart. Follow existing naming (`x.py` ↔ `aiox.py` pattern, e.g., `nextid.py` / `aionextid.py`).

## Testing specifics to keep in mind
- `tests/conftest.py` uses a random DB number between 1–15 to avoid test collisions and installs `uvloop` (async tests expect the `uvloop` event loop to be available).
- README includes doctest examples. `tests/test_doctests.py` executes them — keep README examples accurate and runnable.
- Tests flush DB before/after fixtures; avoid tests that assume a persistent DB state.

## Static checks & style
- The project is typed and ships `py.typed` — preserve type annotations and run `mypy` on your changes.
- Linting/formatting: `flake8` (max complexity 10) and `isort --check-only --diff` are enforced in CI and Makefile.
- Security checks: `bandit --recursive pottery` and `safety scan` are part of the test target and CI.

## Packaging & release
- Version lives in `setup.py` (`__version__`). The `Makefile` includes a `release` target to build `sdist` / `wheel` and upload via `twine`.

## Where to look for examples / patterns
- Core foundation: `pottery/base.py` (encoding, pipeline/watching, container primitives)
- Containers: `pottery/dict.py`, `pottery/set.py`, `pottery/list.py`, `pottery/deque.py`, `pottery/counter.py`, `pottery/queue.py`
- Primitives and async variants: `pottery/redlock.py`, `pottery/aioredlock.py`, `pottery/nextid.py`, `pottery/aionextid.py`
- Tests and fixtures: `tests/` (unit tests & `tests/test_doctests.py`), `tests/conftest.py` for Redis test setup
- CI config: `.github/workflows/python-package.yml`

## Helpful examples for small changes
- Add a new container: mirror patterns in `dict.py` and include tests under `tests/` exercising both sync and async variants (if applicable). Use `_Encodable` for serialization and `_watch` for atomic operations.
- Editing README examples: update `tests/test_doctests.py` (they're executed) and the README doctest blocks.

## Safety & non-goals
- Don't alter public API behavior silently. If an API change is required, add a deprecation note and tests.
- Keep changes typed (mypy clean) and formatted (flake8/isort).

---
If any of these points are unclear or you want short examples for a particular change (e.g., adding an async primitive or adding a pipeline-based operation), tell me which area and I will expand the instructions with concrete code snippets. ✅
