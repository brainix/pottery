# --------------------------------------------------------------------------- #
#   test_doctests.py                                                          #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import doctest
import importlib
import os
import unittest

from tests.base import TestCase  # type: ignore


class DoctestTests(TestCase):  # pragma: no cover
    def _modules(self):
        test_dir = os.path.dirname(__file__)
        package_dir = os.path.dirname(test_dir)
        source_dir = os.path.join(package_dir, 'pottery')
        source_files = (f for f in os.listdir(source_dir) if f.endswith('.py'))
        for source_file in source_files:
            module_name = os.path.splitext(source_file)[0]
            module = importlib.import_module(f'pottery.{module_name}')
            yield module

    @unittest.skip('our doctests run too slowly')
    def test_doctests(self):
        for module in self._modules():
            with self.subTest(module=module):
                results = doctest.testmod(m=module)
                assert not results.failed
