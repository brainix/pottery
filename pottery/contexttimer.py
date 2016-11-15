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
        self._started = None
        self._stopped = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def start(self):
        if self._started:
            raise RuntimeError('contexttimer has already been started')
        elif self._stopped:
            raise RuntimeError('contexttimer has already been stopped')
        else:
            self._started = timeit.default_timer()

    def stop(self):
        if self._stopped:
            raise RuntimeError('contexttimer has already been stopped')
        elif self._started:
            self._stopped = timeit.default_timer()
        else:
            raise RuntimeError("contexttimer hasn't yet been started")

    @property
    def elapsed(self):
        try:
            value = (self._stopped or timeit.default_timer()) - self._started
        except TypeError:
            raise RuntimeError("contexttimer hasn't yet been started")
        else:
            value = round(value * 1000) # rounded to the nearest millisecond
            return value
