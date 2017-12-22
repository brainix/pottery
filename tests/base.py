#-----------------------------------------------------------------------------#
#   base.py                                                                   #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import doctest
import sys
import unittest

from pottery.base import _default_redis



class TestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.redis = _default_redis



def run_doctests():
    results = doctest.testmod()
    sys.exit(bool(results.failed))
