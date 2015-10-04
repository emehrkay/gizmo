"""
Gizmo
------

Gizmo is an object graph mapper for Rexster-based graph servers.

"""
from setuptools import setup, find_packages

setup(
    name             = 'gizmo',
    packages         = find_packages(),
    version          = '0.1.0',
    description      = 'Python OGM for Rexster-based graph servers',
    url              = 'https://github.com/emehrkay/gizmo',
    author           = 'Mark Henderson',
    author_email     = 'emehrkay@gmail.com',
    long_description = __doc__,
    install_requires = [
        'gremlinpy >= 0.2.0',
        'aiogremlin'
    ],
    classifiers      = [
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
    ]
)
