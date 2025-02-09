# --------------------------------------------------------------------------- #
#   test_doctests.py                                                          #
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


import doctest
import importlib
import pathlib
from types import ModuleType
from typing import Generator

import pytest
from redis import Redis


def modules() -> Generator[ModuleType, None, None]:
    tests_dir = pathlib.Path(__file__).parent
    package_dir = tests_dir.parent
    source_dir = package_dir / 'pottery'
    source_files = source_dir.glob('**/*.py')
    for source_file in source_files:
        relative_path = source_file.relative_to(package_dir)
        parts = list(relative_path.parts)
        parts[-1] = source_file.stem
        module_name = '.'.join(parts)
        module = importlib.import_module(module_name)
        yield module


@pytest.mark.parametrize('module', modules())
def test_modules(module: ModuleType) -> None:
    'Run doctests in modules and confirm that they are not science fiction'
    results = doctest.testmod(m=module)
    assert not results.failed


@pytest.fixture
def flush_redis() -> Generator[None, None, None]:
    redis = Redis.from_url('redis://localhost:6379/1')
    redis.flushdb()
    yield
    redis.flushdb()


@pytest.mark.usefixtures('flush_redis')
def test_readme() -> None:
    'Run doctests in README.md and confirm that they are not fake news'
    tests_dir = pathlib.Path(__file__).parent
    package_dir = tests_dir.parent
    readme = str(package_dir / 'README.md')
    results = doctest.testfile(readme, module_relative=False)
    assert not results.failed
