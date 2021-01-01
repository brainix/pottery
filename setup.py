# --------------------------------------------------------------------------- #
#   setup.py                                                                  #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


import os

from setuptools import find_packages
from setuptools import setup

import pottery


package_dir = os.path.dirname(__file__)
readme = os.path.join(package_dir, 'README.md')
with open(readme, encoding='utf-8') as f:
    long_description = f.read()


setup(
    name=pottery.__name__,
    version=pottery.__version__,
    description=pottery.__description__,
    long_description=long_description,
    long_description_content_type='text/markdown',
    url=pottery.__url__,
    author=pottery.__author__,
    author_email=pottery.__author_email__,
    license=pottery.__license__,
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Development Status :: 4 - Beta',
        'Topic :: Database :: Front-Ends',
        'Topic :: System :: Distributed Computing',
        'Topic :: Utilities',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Typing :: Typed',
    ],
    keywords=pottery.__keywords__,
    packages=find_packages(exclude=('contrib', 'docs', 'tests*')),
    install_requires=('redis>=3.4.1', 'mmh3', 'typing_extensions'),
    extras_require={},
    package_data={'pottery': ('py.typed',)},
    data_files=tuple(),
    entry_points={},
)
