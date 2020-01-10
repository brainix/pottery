# --------------------------------------------------------------------------- #
#   test_source.py                                                            #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #
'Python source code tests.'


import itertools
import os
import sys
import unittest

from isort import SortImports

from pottery import monkey
from tests.base import TestCase


class SourceTests(TestCase):
    _EXCLUDES = (
        '/pottery/__init__.py',
    )

    @unittest.skipIf(
        sys.version_info[:2] == (3, 5),
        'isort is broken on Python 3.5 for no good reason ¯\\_(ツ)_/¯'
    )
    def test_imports(self):
        test_dir = os.path.dirname(__file__)
        test_files = (
            f for f in os.listdir(test_dir, absolute=True) if f.endswith('.py')
        )

        root_dir = os.path.dirname(test_dir)
        root_files = (
            f for f in os.listdir(root_dir, absolute=True)
            if f.endswith('.py')
        )

        source_dir = os.path.join(root_dir, 'pottery')
        source_files = (
            f for f in os.listdir(source_dir, absolute=True)
            if f.endswith('.py')
        )

        for f in itertools.chain(test_files, root_files, source_files):
            if not f.endswith(self._EXCLUDES):
                with self.subTest(f=f):
                    assert SortImports(f, check=True).correctly_sorted
