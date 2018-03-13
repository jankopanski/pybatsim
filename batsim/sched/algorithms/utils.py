"""
    batsim.sched.algorithms.utils
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Utils for implementing scheduling algorithms.

"""

from ..alloc import Allocation
from ..job import Job
from ..utils import increment_float
from ..resource import Resource


def find_resources_without_delaying_priority_jobs(
        scheduler, priority_jobs, resources, job, resources_filter=None,
        respect_deps=False, check_func=None):
    """Helper method to find resources for a job without delaying the jobs given as `priority_jobs`.

    To accomplish this the resources are temporarily reserved and freed later.

    :param scheduler: the scheduler

    :param priority_jobs: the reserved jobs

    :param resources: the resources to use for the scheduling

    :param job: the job to be scheduled without delaying the priorities

    :param resources_filter: the filter to use for the resource filtering
                             (can be used to implement topology aware scheduling)

    :param respect_deps: priority jobs are reserved even if their dependencies are not fulfilled yet.

    :param check_func: a function to check whether or not the job should be scheduled.
                       The signature is: job, res, temporary_allocations; whereas
                       the job is the job to be scheduled, res are the resources
                       found for the job, and temporary_allocations are
                       the allocations of the jobs with allocations in the future
                       (reserved jobs).
    """
    scheduler.debug("Reserve priority jobs {jobs}",
                    jobs=priority_jobs,
                    type="reserve_priority_jobs")

    # Temporarily allocate the priority jobs
    temporary_allocations = []
    for j in priority_jobs:
        time = scheduler.time
        if respect_deps:
            for dep in j.resolved_dependencies:
                if isinstance(dep, Job):
                    if dep.running:
                        time = max(
                            time,
                            increment_float(
                                dep.start_time +
                                dep.requested_time,
                                Resource.TIME_DELTA,
                                True))
                    elif dep.completed:
                        time = max(
                            time,
                            increment_float(
                                dep.finish_time,
                                Resource.TIME_DELTA,
                                True))
                    else:
                        return resources.create()
                else:
                    return resources.create()

        # Allow allocations in the future to find the first fit for all priority
        # jobs
        start_time, res = resources.find_with_earliest_start_time(
            j, allow_future_allocations=True,
            filter=resources_filter,
            time=time)
        assert res, "No future allocations were found in backfilling for priority job"

        a = Allocation(start_time, resources=res, job=j)
        a.allocate_all(scheduler)
        temporary_allocations.append(a)

    scheduler.debug("Search resources for backfilling job {job}",
                    job=job,
                    type="search_backfilling_resources")

    # Search for allocations for the given job (not in the future)
    res = resources.find_sufficient_resources_for_job(
        job, filter=resources_filter)

    if check_func is not None:
        if not check_func(job, res, temporary_allocations):
            res = None

    scheduler.debug("Free priority jobs {jobs}",
                    jobs=priority_jobs,
                    type="free_priority_jobs")

    # Free the temporarily acquired allocations
    for t in temporary_allocations:
        t.free()

    scheduler.debug("Results of resources for backfilling job {job}: {res}",
                    job=job,
                    res=res,
                    type="search_backfilling_resources_result")

    return res


def schedule_jobs_without_delaying_priorities(
        scheduler, priority_jobs, resources, jobs,
        abort_on_first_nonfitting=False, resources_filter=None,
        check_func=None, respect_deps=False, handle_scheduled_func=None):
    """Schedule jobs without delaying given priority jobs.

    :param scheduler: the scheduler

    :param priority_jobs: the reserved jobs

    :param resources: the resources to use for the scheduling

    :param jobs: the jobs to be scheduled without delaying the priorities

    :param abort_on_first_nonfitting: whether or not the scheduling should be aborted on the first
                                      job which does not fit (default: `False`)

    :param resources_filter: the filter to use for the resource filtering
                             (can be used to implement topology aware scheduling)

    :param check_func: a function to check whether or not a job should be scheduled.
                       The signature is: job, res, temporary_allocations; whereas
                       the job is the job to be scheduled, res are the resources
                       found for the job, and temporary_allocations are
                       the allocations of the jobs with allocations in the future
                       (reserved jobs).

    :param respect_deps: priority jobs are reserved even if their dependencies are not fulfilled yet.

    :param handle_scheduled_func: a function which will be given the latest scheduled job
                                  as a parameter
    """

    for job in jobs:
        res = find_resources_without_delaying_priority_jobs(
            scheduler, priority_jobs, resources, job, resources_filter,
            respect_deps, check_func)
        if res:
            job.schedule(res)
            if handle_scheduled_func is not None:
                handle_scheduled_func(job)
            scheduler.info(
                "Scheduled job ({job}) without delaying priorities ({priority_jobs})",
                type="schedule_wihout_delaying",
                job=job,
                priority_jobs=priority_jobs)
        elif abort_on_first_nonfitting:
            break


def helper_find_shared_time(time, res):
    """Helper to find the time which is still free (not allocated) after the given
    time for the whole set of resources combined.

    :param time: the first time step to consider

    :param res: the list of resources:
    """
    if not res:
        return 0
    t = None
    for r in res:
        res_time = r.time_free(time=time)
        if t is None:
            t = res_time
        else:
            t = min(t, res_time)
    return t


def helper_cmp_numbers(n1, n2):
    """Helper to compare two numbers and return `1` if the first number is larger,
    `0` if the numbers are equal, and `-1` otherwise.
    """
    if n1 > n2:
        return 1
    elif n1 < n2:
        return -1
    else:
        return 0


def filter_func_consecutive_resources(
        current_time,
        job,
        min_entries,
        max_entries,
        current_result,
        current_remaining,
        r):
    """`filter_func` function to only add resources which are neighbours of already
    added resources (e.g. `[10, 11, 12]`, but not `[10, 11, 13]`)
    """
    if not current_result:
        return True
    last_id = current_result[-1].id
    return r.id == last_id + 1 or r.id == last_id - 1


def best_find_one_of(*fs):
    """`find_best_func` function to combine multiple `find_best_func` so that
    the set is considered as better if only one function matches.

    :param fs: list of `find_best_func` functions
    """
    def do_find(*args, **kwargs):
        for f in fs:
            if f(*args, **kwargs) > 0:
                return 1
        return 0
    return do_find


def best_find_more_resources(
        current_time,
        job,
        min_entries,
        max_entries,
        best_result,
        best_remaining,
        current_result,
        current_remaining):
    """`find_best_func` function to ensure that the current set of resources
    is maximised.
    """
    return helper_cmp_numbers(len(current_result), len(best_result))


def best_find_more_remaining_resources(
        current_time,
        job,
        min_entries,
        max_entries,
        best_result,
        best_remaining,
        current_result,
        current_remaining):
    """`find_best_func` function to ensure that the remaining set of resources
    is maximised.
    """
    return helper_cmp_numbers(len(current_remaining), len(best_remaining))


def best_find_longest_remaining_time(
        current_time,
        job,
        min_entries,
        max_entries,
        best_result,
        best_remaining,
        current_result,
        current_remaining):
    """`find_best_func` function to ensure that the remaining resources have the
    longest remaining time frame still available.
    """

    return helper_cmp_numbers(
        helper_find_shared_time(
            current_time,
            current_remaining),
        helper_find_shared_time(
            current_time,
            best_remaining))


def best_find_shortest_current_time(
        current_time,
        job,
        min_entries,
        max_entries,
        best_result,
        best_remaining,
        current_result,
        current_remaining):
    """`find_best_func` function to ensure that the resources are considered with the
    shortest time frame which is still able to fit the job.
    """

    return helper_cmp_numbers(
        helper_find_shared_time(
            current_time, best_result), helper_find_shared_time(
            current_time, current_result))


def generate_resources_filter(filter_funcs=[], find_best_funcs=[]):
    """Generate a filter method (`batsim.sched.utils`) to filter the resources.

    :param filter_funcs: filter methods which will be called on each resource whether
                         or not it should be added in the current iteration.
                         Signature: current_time, job, min_entries,
                         max_entries, considered_resources, remaining_resources,
                         current_resource

    :param find_best_funcs: filter methods which will be used to compare found resource
                            sets and find the best resource set amongst them.
                            Signature: current_time, job, min_entries,
                            max_entries, best_considered_resources,
                            best_remaining_resources,
                            current_considered_resources,
                            current_remaining_resources
    """
    def do_filter(
            resources,
            job,
            current_time,
            max_entries=None,
            min_entries=None,
            **kwargs):
        best_result = []
        best_remaining = []

        for i in range(0, len(resources) - min_entries + 1):
            result = []
            remaining = resources[:]
            for r in resources[i:]:
                all_valid = True
                for f in filter_funcs:
                    if not f(
                            current_time,
                            job,
                            min_entries,
                            max_entries,
                            result,
                            remaining,
                            r):
                        all_valid = False
                        break
                if all_valid:
                    result.append(r)
                    remaining.remove(r)
                if len(result) == max_entries:
                    break

            if len(result) >= min_entries:
                if not best_result:
                    best_result = result
                    best_remaining = remaining
                    if not find_best_funcs:
                        break
                else:
                    is_best = False
                    for f in find_best_funcs:
                        fres = f(
                            current_time,
                            job,
                            min_entries,
                            max_entries,
                            best_result,
                            best_remaining,
                            result,
                            remaining)
                        if fres < 0:
                            break
                        elif fres > 0:
                            is_best = True
                            break
                    if is_best:
                        best_result = result
                        best_remaining = remaining
        return best_result
    return do_filter


# Resources filter which will make some general checks to ensure a better
# utilisation of the system.
default_resources_filter = generate_resources_filter(
    [filter_func_consecutive_resources],
    [best_find_more_resources,
        best_find_shortest_current_time,
        best_find_more_remaining_resources,
        best_find_longest_remaining_time])

# Resources filter which will still make some general checks but will lead to
# a shorter scheduling time compared to the default filter.
consecutive_resources_filter = generate_resources_filter(
    [filter_func_consecutive_resources], [])
