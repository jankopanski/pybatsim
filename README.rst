
===============================
Pybatsim
===============================

PyBatsim helps you developing your own scheduler in python!

The library consists of two layers:

1. The low level API `batsim.batsim` which handles the communication with the
   Batsim instance (example scheduler: `schedulers/fillerSched.py`).
2. The high level API `batsim.sched` which contains an object oriented abstraction layer
   to provide a simpler API for accessing data from Batsim and filtering for
   jobs and resources (example scheduler: `schedulers/delayProfilesAsTasks.py`).

Commands
--------

The following commands are provided:

*pybatsim*
    To launch schedulers

*pybatsim-experiment*
    To launch experiments

*pybatsim-postprocess-jobs*
    To manipulate the `out_jobs.csv` file based on data only available in the
    scheduler but not in Batsim.

Batsim Version Compatibilities
------------------------------

As there are different release paces between Batsim and Pybatsim versions, here is a list of compatibilities between the two projects:

    - `Batsim master branch`_ with `Pybatsim master branch`_ (development branches, may be unstable)
    - `Batsim v3_0_0`_ with `Pybatsim v3_0_0`_ (latest major release, stable)
    - `Batsim v2_0_0`_ with `Pybatsim batsim_2_0_compatible`_

Moreover, all notable changes are listed in the `changelog <https://gitlab.inria.fr/batsim/pybatsim/blob/master/CHANGELOG.rst>`_.

Examples
--------

Start a scheduler
~~~~~~~~~~~~~~~~~

See the *schedulers* directory for the available built-in schedulers.
A simple built-in scheduler instance can be executed by calling::

  pybatsim fillerSched
  
This command, however, requires an already running Batsim instance.

The parameter to `pybatsim` can also be a file outside of the project directory
like::

  pybatsim path/to/scheduler.py

Schedulers of the higher level API (`batsim.sched`) can be executed in the same way::

  pybatsim delayProfilesAsTasks
  
This example scheduler will make use of dynamic jobs and convert delay jobs into tasks.
Note that dynamic job submissions have to be enabled in your running Batsim instance to be able to use this scheduler.

To see all available starting options see also::

  pybatsim --help

Run an experiment
~~~~~~~~~~~~~~~~~
  
To run a complete experiment the experiment launcher can be used::

  pybatsim-experiment --verbose sample.expe.json
  
Please note that Batsim has to be installed and the environment has to be set-up for this command to succeed.

Files
-----

*launcher.py*
    This is the main entry point to launch things

*launch_expe.py*
    Provide an easy way to launch a complete simulation.
    Just provide a file that looks like `sample.expe.json` to this script and it will launch Batsim and the scheduler with the right options.

*sample.expe.json*
    See `launch_expe.json`

*batsim/batsim.py*
    This class helps you communicate with the batsim server

*batsim/sched/*
    High level scheduler API
    
*batsim/tools/*
    Tools to start the schedulers or for working with the generated data

*schedulers/*
    Contains all the schedulers. Schedulers name should follow this convention:
    `fooBar.py` contains the `FooBar` classname which has as an ancestor `batsim.batsim.BatsimScheduler`.

*schedulers/fillerSched.py*
    A kind of first fit without topology scheduler

*schedulers/easyBackfill.py*
    EASY backfilling where jobs are seen as rectangles

*schedulers/delayProfilesAsTasks.py*
    A scheduler using the high level scheduler API to split big delay jobs into
    small tasks.

Installation
------------

You can install, upgrade, uninstall PyBatsim with these commands::

  pip install [--user] pybatsim
  pip install [--user] --upgrade pybatsim
  pip uninstall pybatsim

Documentation
-------------

To generate the html documentation use the setup target::

  ./setup.py doc

Testing
-------

To run the test experiments it is preferable to first enter in a nix shell specific for the pybatsim development with the following command::

  nix-shell https://github.com/oar-team/kapack/archive/master.tar.gz -A pybatsim_dev

Then you can run tests with the setup target::

  ./setup.py test --batsim-bin=path/to/batsim/binary


.. _Batsim master branch: https://gitlab.inria.fr/batsim/batsim/tree/master
.. _Pybatsim master branch: https://gitlab.inria.fr/batsim/pybatsim/tree/master
.. _Batsim v3_0_0: https://gitlab.inria.fr/batsim/batsim/tags/v3.0.0
.. _Pybatsim v3_0_0: https://gitlab.inria.fr/batsim/pybatsim/tags/v3.0.0
.. _Batsim v2_0_0: https://gitlab.inria.fr/batsim/batsim/tags/v2.0.0
.. _Pybatsim batsim_2_0_compatible: https://gitlab.inria.fr/batsim/pybatsim/tags/batsim_2.0_compatible
