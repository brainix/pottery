# --------------------------------------------------------------------------- #
#   monkey.py                                                                 #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #
'Monkey patches.'


import logging


_logger = logging.getLogger('pottery')


# Monkey patch os.listdir() to optionally return absolute paths.

import os

def _absolutize(*, files, path=None):
    path = os.path.abspath(path or '.')
    files = [os.path.join(path, f) for f in files]
    return files

def _listdir(path=None, *, absolute=False):
    files = _listdir.listdir(path)
    if absolute:  # pragma: no cover
        files = _absolutize(path=path, files=files)
    return files

_listdir.listdir = os.listdir
os.listdir = _listdir

_logger.info('Monkey patched os.listdir() to optionally return absolute paths')


# lolwut for some reason, Redis doesn't have a __hash__() method.

from redis import Redis

Redis.__hash__ = lambda self: hash(str(self))

_logger.info(
    'Monkey patched Redis.__hash__() in order to be able to put Redis clients '
    'into sets'
)
