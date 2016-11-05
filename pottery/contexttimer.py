#-----------------------------------------------------------------------------#
#   contexttimer.py                                                           #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#
'Context manager to measure the execution time of small code snippets.'



import timeit



class contexttimer:
    'Context manager to measure the execution time of small code snippets.'

    def __init__(self):
        self.start = None
        self.stop = None

    def __enter__(self):
        self.start = timeit.default_timer()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop = timeit.default_timer()

    @property
    def elapsed(self):
        try:
            value = self.stop - self.start  # in seconds
        except TypeError:
            now = timeit.default_timer()
            start = self.start or now
            stop = self.stop or now
            value = stop - start            # in seconds
        value = round(value * 1000)         # rounded to the nearest millisecond
        return value
