"""
    batsim.sched.algorithms.backfilling
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Implementation of a simple backfilling algorithm.

"""

from .filling import filler_sched

from . import utils


def backfilling_jobs_sjf(scheduler, reservation_depth):
    """Backfill jobs using the shortest-job-first strategy (relying on the user guesses for the
    requested time).
    """
    runnable_jobs = scheduler.jobs.runnable

    reserved_jobs = runnable_jobs[:reservation_depth]
    remaining_jobs = runnable_jobs[reservation_depth:]

    # Sort remaining jobs with the shortest jobs first
    for job in remaining_jobs.sorted(lambda j: j.requested_time):
        # Search resources for job which do not delay priority jobs
        res = utils.find_resources_without_delaying_priority_jobs(
            scheduler, reserved_jobs, scheduler.resources, job)
        if res:
            scheduler.info(
                "Scheduled job in backfilling ({job}) instead of priority jobs ({priority_jobs})",
                type="backfilling_schedule_job",
                job=job,
                priority_jobs=reserved_jobs)
            job.schedule(res)


def backfilling_sched(scheduler, strategy=None, reservation_depth=None):
    """Backfilling algorithm using the filler scheduler to run the first jobs and
    using a backfilling strategy to backfill low-priority jobs afterwards.

    :param strategy: the strategy (`str`) or fallback to the shortest-job-first strategy

    :param reservation_depth: the number of priority jobs to be reserved during the backfilling
    """
    strategy = strategy or scheduler.options.get("backfilling_strategy", "sjf")
    reservation_depth = reservation_depth or scheduler.options.get(
        "backfilling_reservation_depth",
        1)

    # Start earlier submitted jobs first until a job doesn't fit.
    filler_sched(scheduler, abort_on_first_nonfitting=True)

    # Do backfilling if there are still runnable jobs and free resources.
    if scheduler.resources.free and len(
            scheduler.jobs.runnable) > reservation_depth:
        if strategy == "sjf":
            backfilling_jobs_sjf(scheduler, reservation_depth)
        else:
            raise NotImplementedError(
                "Unimplemented backfilling strategy: {}".format(strategy))
