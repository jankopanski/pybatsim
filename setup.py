#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
from setuptools import setup, find_packages

requirements = [
    "sortedcontainers",
    "pyzmq",
    "redis"
]

if sys.argv[-1] == 'test':
    test_requirements = [
        'coverage'
    ]
    try:
        modules = map(__import__, test_requirements)
    except ImportError as e:
        err_msg = e.message.replace("No module named ", "")
        msg = "%s is not installed. Install your test requirments." % err_msg
        raise ImportError(msg)
    os.system('cd tests; make')
    sys.exit()

setup(
    name='pybatsim',
    author="Michael Mercier",
    author_email="michael.mercier@inria.fr",
    version=1.1,
    url='https://gitlab.inria.fr/batsim/pybatsim',
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,
    zip_safe=False,
    description="Python scheduler for Batsim",
    keywords='Scheduler',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Clustering',
    ]
)
