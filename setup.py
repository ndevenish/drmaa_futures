#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ["drmaa", "pyzmq", "futures;python_version<\"3\"",
  "dill", "six", "enum34;python_version<\"3\""]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(
    author="Nicholas Devenish",
    author_email='ndevenish@gmail.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    description="Execute python functions on a cluster, with a futures interface",
    install_requires=requirements,
    license="BSD license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='drmaa_futures',
    name='drmaa_futures',
    packages=find_packages(include=['drmaa_futures']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/ndevenish/drmaa_futures',
    version='0.1.0',
    zip_safe=False,
)
