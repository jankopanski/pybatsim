"""
    batsim.sched.algorithms.utils
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Utils for implementing scheduling algorithms.

"""

from ..alloc import Allocation


def find_resources_without_delaying_priority_jobs(
        scheduler, priority_jobs, resources, job):
    """Helper method to find resources for a job without delaying the jobs given as `priority_jobs`.

    To accomplish this the resources are temporarily reserved and freed later. No
    checks on the topolgy and other variables are considered.
    """
    # Temporarily allocate the priority jobs
    temporary_allocations = []
    for j in priority_jobs:
        # Allow allocations in the future to find the first fit for all priority
        # jobs which is possible.
        start_time, res = resources.find_earliest_start_time_and_sufficient_resources_for_job(
            j, allow_future_allocations=True)
        a = Allocation(start_time, j.requested_time, res)
        temporary_allocations.append(a)

    # Search for allocations for the given job (not in the future)
    res = resources.find_sufficient_resources_for_job(job)

    # Free the temporarily acquired allocations
    for t in temporary_allocations:
        t.free()

    return res
