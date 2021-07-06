# --------------------------------------------------------------------------- #
#   test_doctests.py                                                          #
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
import importlib
import os
import pathlib
import unittest

from tests.base import TestCase  # type: ignore


class DoctestTests(TestCase):  # pragma: no cover
    def _modules(self):
        test_dir = pathlib.Path(__file__).parent
        package_dir = test_dir.parent
        source_dir = package_dir / 'pottery'
        source_files = source_dir.glob('*.py')
        for source_file in source_files:
            module_name = source_file.stem
            module = importlib.import_module(f'pottery.{module_name}')
            yield module

    @unittest.skipUnless(
        'TEST_DOCTESTS' in os.environ,
        'our doctests run too slowly',
    )
    def test_doctests(self):
        'Run doctests and confirm that they work and are not science fiction'
        for module in self._modules():
            with self.subTest(module=module):
                results = doctest.testmod(m=module)
                assert not results.failed
