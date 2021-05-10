# --------------------------------------------------------------------------- #
#   timer.py                                                                  #
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
'Measure the execution time of small code snippets.'


import timeit
from types import TracebackType
from typing import Optional
from typing import Type
from typing import overload

from typing_extensions import Literal


class ContextTimer:
    '''Measure the execution time of small code snippets.

    Note that ContextTimer measures wall (real-world) time, not CPU time; and
    that .elapsed() returns time in milliseconds.

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

    __slots__ = ('_started', '_stopped')

    def __init__(self) -> None:
        self._started = 0.0
        self._stopped = 0.0

    def __enter__(self) -> 'ContextTimer':
        self.__start()
        return self

    @overload
    def __exit__(self,
                 exc_type: None,
                 exc_value: None,
                 exc_traceback: None,
                 ) -> Literal[False]:
        raise NotImplementedError

    @overload
    def __exit__(self,
                 exc_type: Type[BaseException],
                 exc_value: BaseException,
                 exc_traceback: TracebackType,
                 ) -> Literal[False]:
        raise NotImplementedError

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 exc_traceback: Optional[TracebackType],
                 ) -> Literal[False]:
        self.__stop()
        return False

    def start(self) -> None:
        if self._stopped:
            raise RuntimeError('timer has already been stopped')
        elif self._started:
            raise RuntimeError('timer has already been started')
        else:
            self._started = timeit.default_timer()

    # Preserve the Open-Closed Principle with name mangling.
    #   https://youtu.be/miGolgp9xq8?t=2086
    #   https://stackoverflow.com/a/38534939
    __start = start

    def stop(self) -> None:
        if self._stopped:
            raise RuntimeError('timer has already been stopped')
        elif self._started:
            self._stopped = timeit.default_timer()
        else:
            raise RuntimeError("timer hasn't yet been started")

    __stop = stop

    def elapsed(self) -> int:
        if self._started:
            elapsed = (self._stopped or timeit.default_timer()) - self._started
            return round(elapsed * 1000)
        else:
            raise RuntimeError("timer hasn't yet been started")


if __name__ == '__main__':
    # Run the doctests in this module with:
    #   $ source venv/bin/activate
    #   $ python3 -m pottery.timer
    #   $ deactivate
    import contextlib
    with contextlib.suppress(ImportError):
        from tests.base import run_doctests  # type: ignore
        run_doctests()
