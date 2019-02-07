#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""
import io
from setuptools import setup, find_packages

install_require = []
with io.open('requirements.txt') as f:
    install_require = [l.strip() for l in f if not l.startswith('#')]


setup_requirements = ['pytest-runner']

test_require = []
with io.open('requirements-dev.txt') as f:
    test_require = [l.strip() for l in f if not l.startswith('#')]

packages = find_packages()
print(packages)

setup(
    author="Eduardo A Solis",
    author_email='esolis@homeaway.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="Stream registry client for python",
    install_requires=install_require,
    long_description="foobar",
    include_package_data=True,
    keywords='stream_registry_python_client',
    name='stream_registry_python_client',
    packages=packages,
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_require,
    url='https://github.com/eleduardo/stream_registry_python_client',
    version='0.1.0',
    zip_safe=False,
)
