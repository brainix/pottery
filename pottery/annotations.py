# --------------------------------------------------------------------------- #
#   annotations.py                                                            #
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
