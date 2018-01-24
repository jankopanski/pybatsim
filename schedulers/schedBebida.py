"""
    schedBebida
    ~~~~~~~~~

    This scheduler is the implementation of the BigData scheduler for the
    Bebida on batsim project.

    It is a Simple fcfs algoritihm.

    It take into account preemption by respounding to Add/Remove resource
    events.

"""

from batsim.sched import Scheduler, Jobs
from batsim.sched.algorithms.filling import filler_sched
from batsim.sched.algorithms.utils import default_resources_filter

from procset import ProcSet
import itertools


def to_set(objects):
    return [o.id for o in objects]


class SchedBebida(Scheduler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.to_be_removed_resources = {}

    def on_remove_resources(self, resources):
        # find the list of jobs that are impacted
        # and kill all those jobs
        #import ipdb; ipdb.set_trace()
        self.to_be_removed_resources[resources] = []
        to_be_killed = Jobs()
        for job in self.jobs.running:
            if ProcSet(*to_set(job.allocation)) & ProcSet.from_str(resources):
                to_be_killed.add(job)
        self._batsim.kill_jobs(to_be_killed)
        self.to_be_removed_resources[resources] = to_be_killed

    def on_add_resources(self, resources):
        # add the resources
        for resource in ProcSet.from_str(resources):
            bat_res = { id: resource }
            self._batsim.resources.append(bat_res)
        # Initialize new API data structure
        self._on_pre_init()

        # find the list of jobs that need more resources
        # kill jobs, so tey will be resubmited taking free resources, until
        # tere is no more resources
        free_resource_nb = len(self.resources.free)
        to_be_killed = []
        for job in self.jobs.running:
            wanted_resource_nb = job.requested_resources - len(job.allocation.resources)
            if wanted_resource_nb > 0:
                to_be_killed.append(job)
                free_resource_nb = free_resource_nb - wanted_resource_nb
            if free_resource_nb <= 0:
                break
        self._batsim.kill_jobs(to_be_killed)

    def on_jobs_killed(self, jobs):
        # check if all jobs associated to one decomission are killed
        #for job in jobs:
        #    for _, to_be_killed in self.to_be_removed_resources.items():
        #        for tbk_job in to_be_killed:
        #            if tbk_job.id == job.id:
        #                del tbk_job
        #for resources, to_be_killed in self.to_be_removed_resources.items():
        #    if to_be_killed == []:
        #        # Nothing to kill any more: delete the resources
        #        for resource in ProcSet.from_str(resources):
        #            del self.resources[resource]
        #        # Notify that the resources was removed
        #        self.notify_resources_removed(resources)

        for resources, to_be_killed in self.to_be_removed_resources.items():
            if to_be_killed == Jobs(from_list=jobs):
                #import ipdb; ipdb.set_trace()
                # Nothing to kill any more: delete the resources
                for resource in ProcSet.from_str(resources):
                    del self.resources[resource]
                # Notify that the resources was removed
                self._batsim.notify_resources_removed(resources)


        # TODO resubmit the job
        # get killed jobs progress and resubmit what's left of the jobs
        # for job in jobs:
        #     job.get_job_data("progress")

    def schedule(self):
        return filler_sched(
            self,
            resources_filter=default_resources_filter,
            abort_on_first_nonfitting=True)
