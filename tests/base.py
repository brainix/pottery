#-----------------------------------------------------------------------------#
#   base.py                                                                   #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



import unittest



class TestCase(unittest.TestCase):
    REDIS_URL = 'http://localhost:6379/'
