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
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Union
from typing import cast

# TODO: When we drop support for Python 3.7, change the following import to:
#   from typing import Final
from typing_extensions import Final


logger: Final[logging.Logger] = logging.getLogger('pottery')
logger.addHandler(logging.NullHandler())


# Monkey patch the JSON encoder to be able to JSONify any instance of any class
# that defines a .to_dict(), .to_list(), or .to_str() method (since the encoder
# already knows how to JSONify dicts, lists, and strings).

def _default(self: Any, obj: Any) -> Dict[str, Any] | List[Any] | str:
    method_names = ('to_dict', 'to_list', 'to_str')
    methods = tuple(getattr(obj.__class__, name, None) for name in method_names)
    methods = tuple(method for method in methods if method is not None)
    if len(methods) > 1:
        methods_defined = ', '.join(
            cast(Callable, method).__qualname__ + '()' for method in methods
        )
        raise TypeError(
            f"{methods_defined} defined; "
            f"don't know how to JSONify {obj.__class__.__name__} objects"
        )
    method = methods[0] if methods else _default.default  # type: ignore
    return_value = method(obj)  # type: ignore
    return cast(Union[Dict[str, Any], List[Any], str], return_value)

import json  # isort: skip
_default.default = json.JSONEncoder().default  # type: ignore
json.JSONEncoder.default = _default  # type: ignore

logger.info(
    'Monkey patched json.JSONEncoder.default() to be able to JSONify any '
    'instance of any class that defines a .to_dict(), .to_list(), or .to_str() '
    'method'
)
