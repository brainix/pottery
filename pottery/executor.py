# --------------------------------------------------------------------------- #
#   executor.py                                                               #
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


import concurrent.futures
from types import TracebackType
from typing import Optional
from typing import Type
from typing import overload

from typing_extensions import Literal


class BailOutExecutor(concurrent.futures.ThreadPoolExecutor):
    '''ThreadPoolExecutor subclass that doesn't wait for futures on .__exit__().

    The beating heart of all consensus based distributed algorithms is to
    scatter a computation across multiple nodes, then to gather their results,
    then to evaluate whether quorum is achieved.

    In some cases, quorum requires gathering all of the nodes' results (e.g.,
    interrogating all nodes for a maximum value for a variable).

    But in other cases, quorum requires gathering only n // 2 + 1 nodes'
    results (e.g., figuring out if > 50% of nodes believe that I'm the owner of
    a lock).

    In the latter case, the desired behavior is for the executor to bail out
    early returning control to the main thread as soon as quorum is achieved,
    while still allowing pending in-flight futures to complete in backgound
    threads.  Python's ThreadPoolExecutor's .__exit__() method waits for
    pending futures to complete before returning control to the main thread,
    preventing bail out:
        https://github.com/python/cpython/blob/212337369a64aa96d8b370f39b70113078ad0020/Lib/concurrent/futures/_base.py
        https://docs.python.org/3.9/library/concurrent.futures.html#concurrent.futures.Executor.shutdown

    This subclass overrides .__exit__() to not wait for pending futures to
    complete before returning control to the main thread, allowing bail out.
    '''

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
        self.shutdown(wait=False)
        return False
