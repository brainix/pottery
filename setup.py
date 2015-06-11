#-----------------------------------------------------------------------------#
#   setup.py                                                                  #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#

from setuptools import find_packages
from setuptools import setup

import pottery

setup(
    name=pottery.__name__,
    version=pottery.__version__,
    description='Redis for Humans',
    long_description="""
        Redis is awesome, but Redis clients are not awesome.  Pottery is a
        Pythonic way to access Redis.  If you know how to use Python dicts and
        sets, then you already know how to use Pottery.
    """,
    url='https://github.com/brainix/pottery',
    author=pottery.__author__,
    author_email='brainix@gmail.com',
    license=pottery.__license__,
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Topic :: Database :: Front-Ends',
        'Topic :: Utilities',
        'Programming Language :: Python :: 3 :: Only',
        'License :: OSI Approved :: Apache Software License',
    ],
    keywords='Redis client persistent storage',
    packages=find_packages(exclude=('contrib', 'docs', 'tests*')),
    install_requires=('redis',),
    extras_require={},
    package_data={},
    data_files=tuple(),
    entry_points={},
)
