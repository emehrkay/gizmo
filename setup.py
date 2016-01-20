"""
Gizmo
------

Gizmo is an object graph mapper for Rexster-based graph servers.

"""
import sys

from setuptools import setup, find_packages

install_requires = [
    'gremlinpy',
    'gremlinrestclient',
    'six',
]

if sys.version_info <= (2, 7):
    install_requires.append('trollius')

setup(
    name = 'gizmo',
    packages = find_packages(),
    version = '0.1.0',
    description = 'Python OGM for Rexster-based graph servers',
    url = 'https://github.com/emehrkay/gizmo',
    author = 'Mark Henderson',
    author_email = 'emehrkay@gmail.com',
    long_description = __doc__,
    install_requires = install_requires,
    classifiers = [
        'License :: OSI Approved :: MIT License',
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 2.7',
        'Environment :: Web Environment',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing',
        'Intended Audience :: Developers',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS',
        'Operating System :: MacOS :: MacOS X',
    ],
    test_suite = 'gizmo.test',
)
