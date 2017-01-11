#-----------------------------------------------------------------------------#
#   base.py                                                                   #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import doctest
import sys
import unittest



class TestCase(unittest.TestCase):
    REDIS_URL = 'http://localhost:6379/'



def run_doctests():
    results = doctest.testmod()
    sys.exit(bool(results.failed))
