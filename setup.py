#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import subprocess
import os
import os.path

from setuptools import setup, find_packages, Command

requirements = [
    "sortedcontainers",
    "pyzmq",
    "redis",
    "click",
    "pandas",
    "docopt==0.6.2"
]

setup_requirements = [
    "coverage",
    "autopep8"
]


class UserCommand(Command):

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run_external_command(self, command, *args, cwd=None):
        p = subprocess.Popen([command] + list(args), cwd=cwd)
        p.communicate()
        if p.returncode != 0:
            print(
                'Command failed with exit code',
                p.returncode,
                file=sys.stderr)
            sys.exit(p.returncode)


class TestCommand(UserCommand):

    description = 'Run tests'
    user_options = []

    def run(self):
        self.run_external_command("make", cwd="tests")


class DocCommand(UserCommand):

    description = 'Generate documentation'
    user_options = []

    def run(self):
        self.run_external_command("make", "clean", cwd="doc")
        self.run_external_command(
            "sphinx-apidoc", "-o", "doc/apidoc", "batsim")
        self.run_external_command(
            "sphinx-apidoc",
            "-o",
            "doc/apidoc",
            "schedulers")
        self.run_external_command("make", "html", cwd="doc")

        import webbrowser
        new = 2  # open in a new tab
        dir_path = os.path.dirname(os.path.realpath(__file__))
        webbrowser.open(
            "file:///" +
            os.path.join(
                dir_path,
                "doc/_build/html/index.html"),
            new=new)


class FormatCommand(UserCommand):

    description = 'Format the source code'
    user_options = [
        ('path=', 'p', 'start directory or file'),
    ]

    def initialize_options(self):
        self.path = "."

    def finalize_options(self):
        if self.path is None:
            raise Exception("Parameter --path is missing")
        elif not os.path.exists(self.path):
            raise Exception("Path {} does not exist".format(self.path))

    def run(self):
        self.run_external_command(
            "autopep8",
            "-i",
            "-r",
            "-j",
            "0",
            "-aaaaaa",
            "--experimental",
            self.path)


setup(
    name='pybatsim',
    author="Michael Mercier",
    author_email="michael.mercier@inria.fr",
    version=2.0,
    url='https://gitlab.inria.fr/batsim/pybatsim',
    packages=find_packages(),
    install_requires=requirements,
    setup_requires=setup_requirements,
    include_package_data=True,
    zip_safe=False,
    description="Python scheduler for Batsim",
    keywords='Scheduler',
    license='LGPLv3',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Clustering',
    ],
    entry_points={
        "console_scripts": [
            "pybatsim=batsim.tools.launcher:main",
            "pybatsim-experiment=batsim.tools.launch_experiments:main",
            "pybatsim-postprocess-jobs=batsim.tools.postprocess_jobs:main"
        ]
    },
    cmdclass={
        'test': TestCommand,
        'format': FormatCommand,
        'doc': DocCommand,
    },
)
