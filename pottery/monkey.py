# --------------------------------------------------------------------------- #
#   monkey.py                                                                 #
#                                                                             #
#   Copyright © 2015-2022, Rajiv Bakulesh Shah, original author.              #
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
'Monkey patches.'


# TODO: When we drop support for Python 3.9, remove the following import.  We
# only need it for X | Y union type annotations as of 2022-01-29.
from __future__ import annotations

import logging

# TODO: When we drop support for Python 3.7, change the following import to:
#   from typing import Final
from typing_extensions import Final


logger: Final[logging.Logger] = logging.getLogger('pottery')
logger.addHandler(logging.NullHandler())


import functools  # isort: skip
import json  # isort: skip
from typing import Any  # isort: skip
from typing import Callable  # isort: skip

class PotteryEncoder(json.JSONEncoder):
    'Custom JSON encoder that can serialize Pottery containers.'

    def default(self, o: Any) -> Any:
        from pottery.base import Container
        if isinstance(o, Container):
            if hasattr(o, 'to_dict'):
                return o.to_dict()  # type: ignore
            if hasattr(o, 'to_list'):  # pragma: no cover
                return o.to_list()  # type: ignore
        return super().default(o)

def _decorate_dumps(func: Callable[..., str]) -> Callable[..., str]:
    'Decorate json.dumps() to use PotteryEncoder by default.'
    @functools.wraps(func)
    def wrapper(*args: Any,
                cls: type[json.JSONEncoder] = PotteryEncoder,
                **kwargs: Any,
                ) -> str:
        return func(*args, cls=cls, **kwargs)
    return wrapper

json.dumps = _decorate_dumps(json.dumps)

logger.info(
    'Monkey patched json.dumps() to be able to JSONify Pottery containers by '
    'default'
)
