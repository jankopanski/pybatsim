"""
    batsim.sched.workloads
    ~~~~~~~~~~~~~~~~~~~~~~

    This modules provides utilities to generate workloads from workload models.

"""

from .workloads import *

__all__ = [
    "JobDescription",
    "WorkloadDescription",
    "generate_workload",
]
