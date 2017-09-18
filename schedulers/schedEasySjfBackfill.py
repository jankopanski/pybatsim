"""
    schedEasySjfBackfill
    ~~~~~~~~~~~~~~~~~~~~

    Shortest-job-first backfilling algoritihm using the pre-defined algorithm
    of the new scheduler api.
"""

from batsim.sched.algorithms.backfilling import backfilling_sched
from batsim.sched.algorithms.utils import consecutive_resources_filter


def SchedEasySjfBackfill(scheduler):
    kwargs = {}

    kwargs["reservation_depth"] = scheduler.options.get(
        "backfilling_reservation_depth", 1)

    strategy = scheduler.options.get("backfilling_strategy", "sjf")
    if strategy == "sjf":
        kwargs["backfilling_sort"] = lambda j: j.requested_time
    else:
        raise NotImplementedError(
            "Unimplemented backfilling strategy: {}".format(strategy))

    backfilling_sched(
        scheduler,
        resources_filter=consecutive_resources_filter,
        **kwargs)
