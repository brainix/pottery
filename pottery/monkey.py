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
# that defines a to_dict(), to_list(), or to_str() method (since the encoder
# already knows how to JSONify dicts, lists, and strings).

def _default(self: Any, obj: Any) -> Union[Dict[str, Any], List[Any], str]:
    func_names = ('to_dict', 'to_list', 'to_str')
    funcs = tuple(getattr(obj.__class__, name, None) for name in func_names)
    funcs = tuple(func for func in funcs if func is not None)
    if len(funcs) > 1:
        funcs_defined = ', '.join(
            cast(Callable, func).__qualname__ + '()' for func in funcs
        )
        raise TypeError(
            f"{funcs_defined} defined; "
            f"don't know how to JSONify {obj.__class__.__name__} objects"
        )
    func = funcs[0] if funcs else _default.default  # type: ignore
    return_value = func(obj)  # type: ignore
    return return_value

import json  # isort: skip
_default.default = json.JSONEncoder().default  # type: ignore
json.JSONEncoder.default = _default  # type: ignore

logger.info(
    'Monkey patched json.JSONEncoder.default() to be able to JSONify any '
    'instance of any class that defines a .to_dict(), .to_list(), or .to_str() '
    'method'
)
