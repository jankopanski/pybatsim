"""
    batsim.sched.algorithms.utils
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Utils for implementing scheduling algorithms.

"""

from ..alloc import Allocation


def find_resources_without_delaying_priority_jobs(
        scheduler, priority_jobs, resources, job):
    temporary_allocations = []
    for j in priority_jobs:
        res, start_time = resources.find_sufficient_resources_for_job_with_earliest_start_time(
            j, allow_future_allocations=True)
        a = Allocation(start_time, j.requested_time, res)
        temporary_allocations.append(a)

    res = resources.find_sufficient_resources_for_job(job)

    for t in temporary_allocations:
        t.free()

    return res
