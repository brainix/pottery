#-----------------------------------------------------------------------------#
#   setup.py                                                                  #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#

from setuptools import find_packages
from setuptools import setup

setup(
    name='pottery',
    version='0.1',
    description='Redis for Humans',
    long_description='',
    url='https://github.com/brainix/pottery',
    author='Rajiv Bakulesh Shah',
    author_email='brainix@gmail.com',
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Topic :: Database :: Front-Ends',
        'Topic :: Utilities',
        'Programming Language :: Python :: 3 :: Only',
        'License :: OSI Approved :: Apache Software License',
    ],
    keywords='Redis persistent storage',
    packages=find_packages(exclude=('contrib', 'docs', 'tests*')),
    install_requires=('redis',),
    extras_require={},
    package_data={},
    data_files=tuple(),
    entry_points={},
)
