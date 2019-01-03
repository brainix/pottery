#-----------------------------------------------------------------------------#
#   setup.py                                                                  #
#                                                                             #
#   Copyright © 2015-2019, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from setuptools import find_packages
from setuptools import setup

import pottery



setup(
    name=pottery.__name__,
    version=pottery.__version__,
    description=pottery.__description__,
    long_description=pottery.__long_description__,
    url=pottery.__url__,
    author=pottery.__author__,
    author_email=pottery.__author_email__,
    license=pottery.__license__,
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Topic :: Database :: Front-Ends',
        'Topic :: Utilities',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: Apache Software License',
    ],
    keywords=pottery.__keywords__,
    packages=find_packages(exclude=('contrib', 'docs', 'tests*')),
    install_requires=('redis>=3.0.0', 'murmurhash3',),
    extras_require={},
    package_data={},
    data_files=tuple(),
    entry_points={},
)
