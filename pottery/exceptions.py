# --------------------------------------------------------------------------- #
#   exceptions.py                                                             #
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


# TODO: When we drop support for Python 3.9, remove the following import.  We
# only need it for X | Y union type annotations as of 2022-01-29.
from __future__ import annotations

from dataclasses import dataclass
from queue import Empty
from typing import Iterable

from redis import Redis
from redis import RedisError


@dataclass
class PotteryError(Exception):
    'Base exception class for Pottery containers.'

    redis: Redis
    key: str | None = None

class KeyExistsError(PotteryError):
    'Initializing a container on a Redis key that already exists.'

class RandomKeyError(PotteryError, RuntimeError):
    "Can't create a random Redis key; all of our attempts already exist."

class QueueEmptyError(PotteryError, Empty):
    'Non-blocking .get() or .get_nowait() called on RedisQueue which is empty.'


@dataclass
class PrimitiveError(Exception):
    'Base exception class for distributed primitives.'

    key: str
    masters: Iterable[Redis]
    redis_errors: Iterable[RedisError] = tuple()

class QuorumNotAchieved(PrimitiveError, RuntimeError):
    'Consensus-based algorithm could not achieve quorum.'

class TooManyExtensions(PrimitiveError, RuntimeError):
    'Redlock has been extended too many times.'

class ExtendUnlockedLock(PrimitiveError, RuntimeError):
    'Attempting to extend an unlocked Redlock.'

class ReleaseUnlockedLock(PrimitiveError, RuntimeError):
    'Attempting to release an unlocked Redlock.'

class QuorumIsImpossible(PrimitiveError, RuntimeError):
    'Too many Redis masters threw RedisErrors; quorum can not be achieved.'


class PotteryWarning(Warning):
    'Base warning class for Pottery containers.'

class InefficientAccessWarning(PotteryWarning):
    'Doing an O(n) Redis operation.'
