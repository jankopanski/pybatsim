"""
    batsim.sched.algorithms.backfilling
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Implementation of a simple backfilling algorithm.

"""

from .filling import filler_sched

from . import utils


def do_backfilling(
        scheduler,
        reservation_depth,
        jobs,
        resources,
        resources_filter=None,
        check_func=None,
        handle_scheduled_func=None,
        respect_deps=False,
        backfilling_sort=None):
    """Helper to implement the backfilling step of backfilling algorithms.

    :param scheduler: the scheduler

    :param reservation_depth: how many jobs should be reserved at the beginning of the queue

    :param jobs: the list of all jobs considering in the priority scheduling and in the
                 backfilling

    :param resources: the resources to use for the scheduling

    :param resources_filter: the filter to use for the resource filtering
                             (can be used to implement topology aware scheduling)

    :param check_func: a function to check whether or not a job should be backfilled.
                       The signature is: job, res, temporary_allocations; whereas
                       the job is the job to be backfilled, res are the resources
                       found for the backfilling job, and temporary_allocations are
                       the allocations of the jobs with allocations in the future
                       (reserved jobs).

    :param handle_scheduled_func: a function which will be given the latest scheduled job
                                  as a parameter

    :param respect_deps: priority jobs are reserved even if their dependencies are not fulfilled yet.

    :param backfilling_sort: after the priority jobs were reserved, this function will
                             be used to call jobs.sorted(f) with it to sort the remaining
                             jobs.
    """

    if respect_deps:
        open_jobs = jobs.open

        reserved_jobs = open_jobs[:reservation_depth]
        remaining_jobs = open_jobs[reservation_depth:].runnable
    else:
        runnable_jobs = jobs.runnable

        reserved_jobs = runnable_jobs[:reservation_depth]
        remaining_jobs = runnable_jobs[reservation_depth:]

    if backfilling_sort:
        remaining_jobs = remaining_jobs.sorted(backfilling_sort)

    utils.schedule_jobs_without_delaying_priorities(
        scheduler,
        reserved_jobs,
        resources,
        remaining_jobs,
        resources_filter=resources_filter,
        check_func=check_func,
        respect_deps=respect_deps,
        handle_scheduled_func=handle_scheduled_func)


def backfilling_sched(
        scheduler,
        reservation_depth=1,
        jobs=None,
        resources=None,
        resources_filter=None,
        filler_check_func=None,
        backfilling_check_func=None,
        handle_scheduled_func=None,
        respect_deps=False,
        backfilling_sort=None,
        priority_sort=None):
    """Backfilling algorithm using the filler scheduler to run the first jobs and backfill the remaining jobs.

    :param scheduler: the scheduler

    :param reservation_depth: how many jobs should be reserved at the beginning of the queue

    :param jobs: the list of all jobs considering in the priority scheduling and in the
                 backfilling

    :param resources: the resources to use for the scheduling

    :param resources_filter: the filter to use for the resource filtering
                             (can be used to implement topology aware scheduling)

    :param filler_check_func: a function which can check whether or not jobs in the filler_sched
                              part are allowed to be scheduled.
                              Signature: job, res, list_of_already_scheduled_jobs

    :param backfilling_check_func: a function to check whether or not a job should be backfilled.
                                   The signature is: job, res, temporary_allocations; whereas
                                   the job is the job to be backfilled, res are the resources
                                   found for the backfilling job, and temporary_allocations are
                                   the allocations of the jobs with allocations in the future
                                   (reserved jobs).

    :param handle_scheduled_func: a function which will be given the latest scheduled job
                                  as a parameter

    :param respect_deps: priority jobs are reserved even if their dependencies are not fulfilled yet.

    :param backfilling_sort: after the priority jobs were reserved, this function will
                             be used to call jobs.sorted(f) with it to sort the remaining
                             jobs.

    :param priority_sort:    this function will be used to call jobs.sorted(f) to sort
                             the initial list of priority jobs.
    """

    if jobs is None:
        jobs = scheduler.jobs

    if priority_sort:
        jobs = jobs.sorted(priority_sort)

    if resources is None:
        resources = scheduler.resources.compute

    # Start earlier submitted jobs first until a job doesn't fit.
    filler_sched(scheduler, abort_on_first_nonfitting=True, jobs=jobs,
                 resources=resources, resources_filter=resources_filter,
                 check_func=filler_check_func,
                 respect_deps=respect_deps,
                 handle_scheduled_func=handle_scheduled_func)

    # Do backfilling if there are still runnable jobs and free resources.
    if len(jobs.runnable) > reservation_depth:
        do_backfilling(scheduler, reservation_depth, jobs=jobs,
                       resources=resources, resources_filter=resources_filter,
                       check_func=backfilling_check_func,
                       handle_scheduled_func=handle_scheduled_func,
                       respect_deps=respect_deps,
                       backfilling_sort=backfilling_sort)
