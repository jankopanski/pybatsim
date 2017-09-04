"""
    batsim.sched.algorithms.filling
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Implementation of a simple job filling algorithm.

"""


def filler_sched(scheduler, abort_on_first_nonfitting=False, jobs=None):
    """Filler algorithm which will try to schedule all jobs but will reject jobs which will
    never fit into the machine.

    :param abort_on_first_nonfitting: whether or not the filling should be aborted on the first
    job which does not fit (default: `False`)
    """

    abort_on_first_nonfitting = (
        abort_on_first_nonfitting or scheduler.options.get(
            "filler_sched_abort_on_first_nonfitting", False))

    if jobs is None:
        jobs = scheduler.jobs

    for job in jobs.runnable:
        res = scheduler.resources.find_sufficient_resources_for_job(job)
        if res:
            job.schedule(res)
        elif job.requested_resources > len(scheduler.resources):
            job.reject(
                "Too few resources available in the system (overall)")
        else:
            if abort_on_first_nonfitting:
                break
