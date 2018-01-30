"""
    schedBebida
    ~~~~~~~~~

    This scheduler is the implementation of the BigData scheduler for the
    Bebida on batsim project.

    It is a Simple fcfs algoritihm.

    It take into account preemption by respounding to Add/Remove resource
    events.

"""

from batsim.batsim import BatsimScheduler

from procset import ProcSet
import itertools


class SchedBebida(BatsimScheduler):

    def filter_jobs_by_state(self, state):
        return [job for job in self.jobs if job.job_state == state]

    def runnning_jobs(self):
        return filter_jobs_by_state(Job.state.RUNNING)

    def submitted_jobs(self):
        return filter_jobs_by_state(Job.state.SUBMITTED)

    def allocate_first_fit_in_best_effort(job):
        """
        return the allocation with as much resources as possible up to
        the job's `requeqted_resources` number.
        return None if no resources at all are available.
        """
        logger.info("Try to allocate Job: {}".format(job.id));
        nb_found_resources = 0
        allocation = ProcSet()
        nb_resources_still_needed = job.requested_resources

        curr_interval = next(self.free_resources.intervals)

        while (len(allocation) < job.requested_resources or curr_interval is None):
            interval_size = len(interval);

            if interval_size > nb_resources_still_needed:
                allocation = allocation + ProcInt(
                        inf=curr_interval.inf,
                        sup=(curr_interval + nb_resources_still_needed -1))
            elif interval_size == nb_resources_still_needed:
                allocation.add(ProcInt(*curr_interval))
            elif interval_size < nb_resources_still_needed:
                allocation.add(ProcInt(*curr_interval))
                nb_resources_still_needed = nb_resources_still_needed - interval_size
                curr_interval = next(self.free_resources.intervals)
        job.allocation = allocation
        logger.info("Allocation for job {}: {}".format(job.id, job.allocation));


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.to_be_removed_resources = {}

    def onSimulationBegins(self):
        self.free_resources = ProcSet(*[res["id"] for res in self.resources])


    def onRemoveResources(self, resources):
        # find the list of jobs that are impacted
        # and kill all those jobs
        #import ipdb; ipdb.set_trace()
        self.to_be_removed_resources[resources] = []
        to_be_killed = []
        for job in self.running_jobs():
            if job.allocation & ProcSet.from_str(resources):
                to_be_killed.add(job)
        self._batsim.kill_jobs(to_be_killed)
        self.to_be_removed_resources[resources] = to_be_killed

    def onAddResources(self, resources):
        # add the resources
        for resource in ProcSet.from_str(resources):
            #import ipdb; ipdb.set_trace()
            self.resources[resource].unavailable = False

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

    def onJobsKilled(self, jobs):
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
        # Implement a simple FIFO scheduler
        print("Do schedule")
        for job in self.submitted_jobs():

