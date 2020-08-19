# --------------------------------------------------------------------------- #
#   monkey.py                                                                 #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #
'Monkey patches.'


import logging
from typing import Any
from typing import cast

from typing_extensions import Final


_logger: Final[logging.Logger] = logging.getLogger('pottery')


# The Redis client connection pool doesn't have a sane equality test.  So
# monkey patch equality comparisons on to the connection pool.  We consider two
# connection pools to be equal if they're connected to the same host, port, and
# database.

from redis import ConnectionPool  # isort:skip

def __eq__(self: ConnectionPool, other: Any) -> bool:
    try:
        return cast(bool, self.connection_kwargs == other.connection_kwargs)
    except AttributeError:  # pragma: no cover
        return False

ConnectionPool.__eq__ = __eq__  # type: ignore

_logger.info(
    'Monkey patched ConnectionPool.__eq__() to compare clients by connection '
    'params'
)
