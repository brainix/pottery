#-----------------------------------------------------------------------------#
#   base.py                                                                   #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import doctest
import functools
import sys
import unittest
import warnings

from redis import Redis



class TestCase(unittest.TestCase):
    REDIS_URL = 'http://localhost:6379/'

    def setUp(self):
        'Set up a unit test.'
        super().setUp()
        self.redis = Redis()



def ignore_warnings(func):
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return func(*args, **kwargs)
    return wrap



def run_doctests():
    results = doctest.testmod()
    sys.exit(bool(results.failed))
