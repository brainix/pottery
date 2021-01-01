# --------------------------------------------------------------------------- #
#   annotations.py                                                            #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import TypeVar
from typing import Union


# A function that receives *args and **kwargs, and returns anything.  Useful
# for annotating decorators.
F = TypeVar('F', bound=Callable[..., Any])


JSONTypes = Union[None, bool, int, float, str, List[Any], Dict[str, Any]]
RedisValues = Union[bytes, str, float, int]
