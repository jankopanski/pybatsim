"""
    fcfs2
    ~~~~~

    Simple fcfs algoritihm using the pre-defined algorithm of the new scheduler api.

"""

from batsim.sched.algorithms.filling import filler_sched


def Fcfs2(scheduler):
    return filler_sched(scheduler, abort_on_first_nonfitting=True)
