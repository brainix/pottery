#-----------------------------------------------------------------------------#
#   timer.py                                                                  #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#
'Measure the execution time of small code snippets.'



import timeit



class ContextTimer:
    '''Measure the execution time of small code snippets.

    Note that ContextTimer measures wall (real-world) time, not CPU time; and
    that elapsed() returns time in milliseconds.

    You can use ContextTimer stand-alone...

        >>> import time
        >>> timer = ContextTimer()
        >>> timer.start()
        >>> time.sleep(0.1)
        >>> 100 <= timer.elapsed() < 200
        True
        >>> timer.stop()
        >>> time.sleep(0.1)
        >>> 100 <= timer.elapsed() < 200
        True

    ...or as a context manager:

        >>> tests = []
        >>> with ContextTimer() as timer:
        ...     time.sleep(0.1)
        ...     tests.append(100 <= timer.elapsed() < 200)
        >>> time.sleep(0.1)
        >>> tests.append(100 <= timer.elapsed() < 200)
        >>> tests
        [True, True]
    '''

    def __init__(self):
        self._started = None
        self._stopped = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def start(self):
        if self._stopped:
            raise RuntimeError('timer has already been stopped')
        elif self._started:
            raise RuntimeError('timer has already been started')
        else:
            self._started = timeit.default_timer()

    def stop(self):
        if self._stopped:
            raise RuntimeError('timer has already been stopped')
        elif self._started:
            self._stopped = timeit.default_timer()
        else:
            raise RuntimeError("timer hasn't yet been started")

    def elapsed(self):
        try:
            value = (self._stopped or timeit.default_timer()) - self._started
        except TypeError:
            raise RuntimeError("timer hasn't yet been started")
        else:
            value = round(value * 1000) # rounded to the nearest millisecond
            return value



if __name__ == '__main__':  # pragma: no cover
    # Run the doctests in this module with:
    #   $ source venv/bin/activate
    #   $ python3 -m pottery.timer
    #   $ deactivate
    import contextlib
    with contextlib.suppress(ImportError):
        from tests.base import run_doctests
        run_doctests()
