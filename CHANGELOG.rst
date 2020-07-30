.. _changelog:

Changelog
=========

All notable changes to this project will be documented in this file.
The format is based on `Keep a Changelog`_.

........................................................................................................................

Unreleased
----------

- `Commits since v3.2.0 <https://gitlab.inria.fr/batsim/pybatsim/compare/v3.2.0...master>`_
- ``nix-env -f https://github.com/oar-team/kapack/archive/master.tar.gz -iA pybatsim_dev``


........................................................................................................................

v3.2.0
------

- Release date: 2020-07-30
- `Commits since v3.1.0 <https://gitlab.inria.fr/batsim/pybatsim/compare/v3.1.0...v3.2.0>`_
- ``nix-env -f https://github.com/oar-team/kapack/archive/master.tar.gz -iA pybatsim320``
- Recommended Batsim version: `v4.0.0 <https://gitlab.inria.fr/batsim/batsim/tags/v4.0.0>`_

This version is synchronized with Batsim v4.0.0.
See `Batsim changelog <https://batsim.readthedocs.io/en/latest/changelog.html#v4-0-0>`_ for more details.

Additions
~~~~~~~~~

- Added the handling of `machine_available` and `machine_unavailable` events, as well as the unknown external events.
- The `storage_mapping` of a job is now attached to an `EXECUTE_JOB` event.
- The `register_job` function now returns the created Job object.
- Added the `StorageController` permitting to easily manage `storage` resources and manipulate data between them with `data_staging` jobs.
  To be used, the StorageController object must be instanciated by the scheduler after the `SIMULATION_BEGINS` event.
- Added the `reject_jobs_by_ids` function to reject jobs by giving a list of job ids, as opposed to `reject_jobs` which expects a list of Job objects.

Miscellaneous
~~~~~~~~~~~~~

- Example schedulers in the `schedulers` folder are now up-to-date, except from those in the `unMaintained` sub-folder.


........................................................................................................................

v3.1.0
------

- Release date: 2019-01-18
- `Commits since v2.1.1 <https://gitlab.inria.fr/batsim/pybatsim/compare/2.1.1...v3.1.0>`_
- ``nix-env -f https://github.com/oar-team/kapack/archive/master.tar.gz -iA pybatsim3``
- Recommended Batsim version: `v3.0.0 <https://gitlab.inria.fr/batsim/batsim/tags/v3.0.0>`_

This version is synchronized with Batsim v3.0.0.
See `Batsim changelog <https://batsim.readthedocs.io/en/latest/changelog.html#v3-0-0>`_ for more details.

Changes in API
~~~~~~~~~~~~~~

- Mark `start_jobs` as DEPRECATED, please now use `execute_jobs`.
- `set_resource_state`, `notify_resources_added` and `notify_resources_removed` functions now expect a ProcSet for the `resources` argument.
- `onAddResources` and `onRemoveResources` now sends a ProcSet for the `to_add` and `to_remove` arguments, respectively.


........................................................................................................................

v2.1.1
------

- Release date: 2018-08-31
- `Commits since v2.0.0 <https://gitlab.inria.fr/batsim/pybatsim/compare/2.0...2.1.1>`_
- ``nix-env -f https://github.com/oar-team/kapack/archive/master.tar.gz -iA pybatsim2``
- Recommended Batsim version: `v2.0.0 <https://gitlab.inria.fr/batsim/batsim/tags/v2.0.0>`_

........................................................................................................................

v2.0.0
------

- Release date: 2017-10-03
- Recommended Batsim version: `v2.0.0 <https://gitlab.inria.fr/batsim/batsim/tags/v2.0.0>`_




.. _Keep a Changelog: http://keepachangelog.com/en/1.0.0/