"""
    batsim.sched.algorithms.filling
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Implementation of a simple job filling algorithm.

"""


def filler_sched(scheduler, abort_on_first_nonfitting=False, jobs=None,
                 resources=None,
                 resources_filter=None, check_func=None,
                 respect_deps=False,
                 handle_scheduled_func=None):
    """Helper to implement a filler scheduling algorithm.

    :param scheduler: the scheduler

    :param abort_on_first_nonfitting: whether or not the filling should be aborted on the first
                                      job which does not fit (default: `False`)

    :param jobs: the list of all jobs considering in the priority scheduling and in the
                 backfilling

    :param resources: the resources to use for the scheduling

    :param resources_filter: the filter to use for the resource filtering
                             (can be used to implement topology aware scheduling)

    :param check_func: a function to check whether or not a job is allowed to be scheduled.
                       Signature: job, res, list_of_already_scheduled_jobs

    :param respect_deps: priority jobs are reserved even if their dependencies are not fulfilled yet.

    :param handle_scheduled_func: a function which will be given the latest scheduled job
                                  as a parameter
    """

    abort_on_first_nonfitting = (
        abort_on_first_nonfitting or scheduler.options.get(
            "filler_sched_abort_on_first_nonfitting", False))

    if jobs is None:
        jobs = scheduler.jobs

    if resources is None:
        resources = scheduler.resources.compute

    already_scheduled = []

    for job in jobs.open:
        if not job.runnable:
            if abort_on_first_nonfitting:
                if respect_deps:
                    break
                else:
                    continue
            else:
                continue
        res = resources.find_sufficient_resources_for_job(
            job, filter=resources_filter)
        if res:
            if check_func is not None:
                if not check_func(job, res, already_scheduled):
                    if abort_on_first_nonfitting:
                        break
                    else:
                        continue
            job.schedule(res)
            if handle_scheduled_func is not None:
                handle_scheduled_func(job)
            already_scheduled.append(job)
        elif job.requested_resources > len(resources):
            job.reject(
                "Too few resources available in the system (overall)")
        else:
            if abort_on_first_nonfitting:
                break
