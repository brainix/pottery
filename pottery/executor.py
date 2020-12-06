# --------------------------------------------------------------------------- #
#   executor.py                                                               #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import concurrent.futures


class BailOutExecutor(concurrent.futures.ThreadPoolExecutor):
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(wait=False)
        return False
