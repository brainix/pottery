# --------------------------------------------------------------------------- #
#   monkey.py                                                                 #
#                                                                             #
#   Copyright © 2015-2021, original authors.                                  #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #
'Monkey patches.'


import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Union

from typing_extensions import Final


_logger: Final[logging.Logger] = logging.getLogger('pottery')


# The Redis client connection pool doesn't have a sane equality test.  So
# monkey patch equality comparisons on to the connection pool.  We consider two
# connection pools to be equal if they're connected to the same host, port, and
# database.

from redis import ConnectionPool  # isort:skip

def __eq__(self: ConnectionPool, other: Any) -> bool:
    try:
        value: bool = self.connection_kwargs == other.connection_kwargs
    except AttributeError:  # pragma: no cover
        value = False
    return value

ConnectionPool.__eq__ = __eq__  # type: ignore

_logger.info(
    'Monkey patched ConnectionPool.__eq__() to compare clients by connection '
    'params'
)


# Monkey patch the JSON encoder to be able to JSONify any instance of any class
# that defines a to_dict(), to_list(), or to_str() method (since the encoder
# already knows how to JSONify dicts, lists, and strings).

def _default(self: Any, obj: Any) -> Union[Dict[str, Any], List[Any], str]:
    func_names = {'to_dict', 'to_list', 'to_str'}
    funcs = {getattr(obj.__class__, name, None) for name in func_names}
    funcs.discard(None)
    assert len(funcs) <= 1
    func = funcs.pop() if any(funcs) else _default.default  # type: ignore
    return_value: Union[Dict[str, Any], List[Any], str] = func(obj)
    return return_value

import json  # isort:skip
_default.default = json.JSONEncoder().default  # type: ignore
json.JSONEncoder.default = _default  # type: ignore

_logger.info(
    'Monkey patched json.JSONEncoder.default() to be able to JSONify any '
    'instance of any class that defines a to_dict(), to_list(), or to_str() '
    'method'
)
