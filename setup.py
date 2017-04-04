#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import os.path as op


from codecs import open
from setuptools import setup, find_packages

requirements = [
    "sortedcontainers",
    "pyzmq",
    "redis"
]

version = '1.0'

setup(
    name='pybatsim',
    author="David Glesser",
    version=version,
    url='git@gitlab.inria.fr:batsim/pybatsim.git',
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,
    zip_safe=False,
    description="Python scheduler for Batsim",
    keywords='Scheduler',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Clustering',
    ]
)
