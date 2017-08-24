"""
    batsim.sched.algorithms.filling
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Implementation of a simple job filling algorithm.

"""


def filler_sched(scheduler, abort_on_first_nonfitting=False):
    abort_on_first_nonfitting = (
        abort_on_first_nonfitting or scheduler.options.get(
            "filler_sched_abort_on_first_nonfitting", False))

    for job in scheduler.jobs.runnable:
        res = scheduler.resources.find_sufficient_resources_for_job(job)
        if res:
            job.schedule(res)
        elif job.requested_resources > len(scheduler.resources):
            job.reject(
                "Too few resources available in the system (overall)")
        else:
            if abort_on_first_nonfitting:
                break
