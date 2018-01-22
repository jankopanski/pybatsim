"""
    schedBebida
    ~~~~~~~~~

    This scheduler is the implementation of the BigData scheduler for the
    Bebida on batsim project.

    It is a Simple fcfs algoritihm.

    It take into account preemption by respounding to Add/Remove resource
    events.

"""

from batsim.sched import Scheduler
from batsim.sched.algorithms.filling import filler_sched
from batsim.sched.algorithms.utils import default_resources_filter

from procset import ProcSet
import itertools


def to_set(objects):
    return [o.id for o in objects]


class SchedBebida(Scheduler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.to_be_removed_resources = []

    def on_remove_resources(self, resources):
        # find the list of jobs that are impacted
        # and kill all those jobs
        #import ipdb; ipdb.set_trace()
        for job in self.jobs.running:
            if ProcSet(*to_set(job.allocation)) & ProcSet.from_str(resources):
                job.kill()
        self.to_be_removed_resources.append(resources)

    def on_add_resources(self, resources):
        # add the resources
        for resource in ProcSet(*to_set(resources)):
            bat_res = { id: resource }
            self._batsim.resources.append(bat_res)
        # Initialize new API data structure
        self._on_pre_init()

        # find the list of jobs that need more resources
        # kill jobs, so tey will be resubmited taking free resources, until
        # tere is no more resources
        free_resource_nb = len(self.resources.free)
        for job in self.jobs.running:
            wanted_resource_nb = job.requested_resources - len(job.allocation.resources)
            if wanted_resource_nb > 0:
                job.kill()
                free_resource_nb = free_resource_nb - wanted_resource_nb
            if free_resource_nb <= 0:
                break

    def on_jobs_killed(self, jobs):
        # Do remove resources that was decommisionned
        for resouce in self.to_be_removed_resources:
            del self.resources[resource]
        self.to_be_removed_resources = []

        # resubmit the job

        # TODO get killed jobs progress and resubmit what's left of the jobs
        # for job in jobs:
        #     job.get_job_data("progress")

    def schedule(self):
        return filler_sched(
            self,
            resources_filter=default_resources_filter,
            abort_on_first_nonfitting=True)
