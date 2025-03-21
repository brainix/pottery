# --------------------------------------------------------------------------- #
#   setup.py                                                                  #
#                                                                             #
#   Copyright © 2015-2025, Rajiv Bakulesh Shah, original author.              #
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
'''Redis for Humans.

Redis is awesome, but Redis commands are not always intuitive.  Pottery is a
Pythonic way to access Redis.  If you know how to use Python dicts, then you
already know how to use Pottery.  Pottery is useful for accessing Redis more
easily, and also for implementing microservice resilience patterns; and it has
been battle tested in production at scale.
'''


import pathlib

from setuptools import find_packages
from setuptools import setup


__title__ = 'pottery'
__version__ = '3.0.1'
__description__ = __doc__.split(sep='\n\n', maxsplit=1)[0]
__url__ = 'https://github.com/brainix/pottery'
__author__ = 'Rajiv Bakulesh Shah'
__author_email__ = 'brainix@gmail.com'
__keywords__ = 'Redis client persistent storage'
__copyright__ = f'Copyright © 2015-2025, {__author__}, original author.'


_package_dir = pathlib.Path(__file__).parent
_long_description = (_package_dir / 'README.md').read_text()


setup(
    version=__version__,
    description=__description__,
    long_description=_long_description,
    long_description_content_type='text/markdown',
    url=__url__,
    author=__author__,
    author_email=__author_email__,
    classifiers=[
        'Intended Audience :: Developers',
        'Development Status :: 4 - Beta',
        'Topic :: Database :: Front-Ends',
        'Topic :: System :: Distributed Computing',
        'Topic :: Utilities',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Framework :: AsyncIO',
        'Typing :: Typed',
    ],
    keywords=__keywords__,
    python_requires='>=3.9, <4',
    install_requires=('redis>=4.2.0rc1', 'mmh3', 'typing_extensions'),
    extras_require={},
    packages=find_packages(exclude=('contrib', 'docs', 'tests*')),
    package_data={'pottery': ['py.typed']},
    data_files=[],
    entry_points={},
)
