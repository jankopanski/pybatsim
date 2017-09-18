"""
    schedFiller
    ~~~~~~~~~~~

    Job filling algoritihm using the pre-defined algorithm of the new scheduler api.

"""

from batsim.sched.algorithms.filling import filler_sched
from batsim.sched.algorithms.utils import default_resources_filter


def SchedFiller(scheduler):
    return filler_sched(
        scheduler,
        resources_filter=default_resources_filter,
        abort_on_first_nonfitting=False)
