.. _changelog:

Changelog
=========

All notable changes to this project will be documented in this file.
The format is based on `Keep a Changelog`_.

........................................................................................................................

Unreleased
----------

- `Commits since v3.0.0 <https://gitlab.inria.fr/batsim/pybatsim/compare/v3.0.0...master>`_
- ``nix-env -f https://github.com/oar-team/kapack/archive/master.tar.gz -iA pybatsim_dev``

Additions
~~~~~~~~~

- Added the handling of `machine_available` and `machine_unavailable` events.
- `storage_mapping` of a job is not attached to an `EXECUTE_JOB` event.
- `register_job` function now returns the created Job.

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