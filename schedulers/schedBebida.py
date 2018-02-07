"""
    schedBebida
    ~~~~~~~~~

    This scheduler is the implementation of the BigData scheduler for the
    Bebida on batsim project.

    It is a Simple fcfs algoritihm.

    It take into account preemption by respounding to Add/Remove resource
    events. It kills the jobs that are allocated to removed resources. It also
    kill some jobs in the queue in order to re-schedule them on a larger set of
    resources.

    The Batsim job profile "msg_hg_tot" is MANDATORY for this mechanism to work.
    Also, the batsim option  "job_submission":{"forward_profiles":true} is mandatory

"""

from batsim.batsim import BatsimScheduler, Job

from procset import ProcSet, ProcInt
import logging


class SchedBebida(BatsimScheduler):

    def filter_jobs_by_state(self, state):
        return [job for job in self.bs.jobs.values() if job.job_state == state]

    def running_jobs(self):
        return self.filter_jobs_by_state(Job.State.RUNNING)

    def submitted_jobs(self):
        return self.filter_jobs_by_state(Job.State.SUBMITTED)

    def allocate_first_fit_in_best_effort(self, job):
        """
        return the allocation with as much resources as possible up to
        the job's `requeqted_resources` number.
        return None if no resources at all are available.
        """
        self.logger.info("Try to allocate Job: {}".format(job.id))
        assert(job.allocation is None ,
               "Job allocation should be None and not {}".format(job.allocation))

        nb_found_resources = 0
        allocation = ProcSet()
        nb_resources_still_needed = job.requested_resources

        iter_intervals = self.free_resources.intervals()
        curr_interval = next(iter_intervals)

        while (len(allocation) < job.requested_resources
               and curr_interval is not None):
            #import ipdb; ipdb.set_trace()
            interval_size = len(curr_interval)
            self.logger.debug("Interval lookup: {}".format(curr_interval))
            #self.logger.debug("Interval lookup: {}".format(curr_interval))

            if interval_size > nb_resources_still_needed:
                allocation.insert(
                    ProcInt(
                        inf=curr_interval.inf,
                        sup=(curr_interval.inf + nb_resources_still_needed -1))
                )
            elif interval_size == nb_resources_still_needed:
                allocation.insert(ProcInt(*curr_interval))
            elif interval_size < nb_resources_still_needed:
                allocation.insert(ProcInt(*curr_interval))
                nb_resources_still_needed = nb_resources_still_needed - interval_size
                try:
                    curr_interval = next(iter_intervals)
                except StopIteration:
                    curr_interval = None
        job.allocation = allocation
        job.state = Job.State.RUNNING

        # udate free resources
        self.free_resources = self.free_resources - job.allocation

        self.logger.info("Allocation for job {}: {}".format(
            job.id, job.allocation))


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.to_be_removed_resources = {}
        self.submitted_profiles = {}

    def onSimulationBegins(self):
        self.free_resources = ProcSet(*[res_id for res_id in
            self.bs.resources.keys()])
        assert self.bs.batconf["job_submission"]["forward_profiles"] == True, (
                "Forward profile is mandatory for resubmit to work")

    def onJobSubmission(self, job):
        assert "type" in job.profile_dict, "Forward profile is mandatory"
        assert (job.profile_dict["type"] == "msg_par_hg_tot")

    def onJobCompletion(self, job):
        # udate free resources
        self.free_resources = self.free_resources | job.allocation

    def onNoMoreEvents(self):
        if len(self.free_resources) > 0:
            self.schedule()

        self.logger.debug("=====================NO MORE EVENTS======================")
        self.logger.debug("\nFREE RESOURCES = {}".format(str(self.free_resources)))

        self.logger.debug(("\nSUBMITTED JOBS = {}\n"
                           "SCHEDULED JOBS = {}\n"
                           "COMPLETED JOBS = {}").format(
                               self.bs.nb_jobs_received,
                               self.bs.nb_jobs_scheduled,
                               self.bs.nb_jobs_completed))
        self.logger.debug("\nJOBS: \n{}".format(self.bs.jobs))

    def onRemoveResources(self, resources):
        # find the list of jobs that are impacted
        # and kill all those jobs
        self.to_be_removed_resources[resources] = []
        to_be_killed = []
        for job in self.running_jobs():
            if job.allocation & ProcSet.from_str(resources):
                to_be_killed.append(job)
        self.bs.kill_jobs(to_be_killed)
        self.to_be_removed_resources[resources] = to_be_killed

    def onAddResources(self, resources):
        # add the resources
        self.free_resources = self.free_resources | ProcSet.from_str(resources)

        # self.bs.notify_resources_added(resources)

        # find the list of jobs that need more resources
        # kill jobs, so tey will be resubmited taking free resources, until
        # tere is no more resources
        free_resource_nb = len(self.free_resources)
        to_be_killed = []

        for job in self.running_jobs():
            wanted_resource_nb = job.requested_resources - len(job.allocation)
            if wanted_resource_nb > 0:
                to_be_killed.append(job)
                free_resource_nb = free_resource_nb - wanted_resource_nb
            if free_resource_nb <= 0:
                break
        if len(to_be_killed) > 0:
            self.bs.kill_jobs(to_be_killed)

        self.schedule()

    def onJobsKilled(self, jobs):
        # check if all jobs associated to one decomission are killed
        #for job in jobs:
        #    for _, to_be_killed in self.to_be_removed_resources.items():
        #        for tbk_job in to_be_killed:
        #            if tbk_job.id == job.id:
        #                del tbk_job
        #for resources, to_be_killed in self.to_be_removed_resources.items():
        #    if to_be_killed == []:

        for resources, to_be_killed in self.to_be_removed_resources.items():
            if len(to_be_killed) > 0 and to_be_killed == jobs:
                # Nothing to kill any more: delete the resources
                self.free_resources = self.free_resources - ProcSet.from_str(resources)
                # Notify that the resources was removed
                self.bs.notify_resources_removed(resources)
                to_remove.append(resources)

        # get killed jobs progress and resubmit what's left of the jobs
        for job in jobs:
            progress = job.progress
            if "current_task_index" in progress:
                curr_task = progress["current_task_index"]
                # TODO get profile to resubmit current and following sequential
                # tasks
            # TODO Submit the profile if not already done
            self.bs.resubmit_job(job)

    def onDeadlock(self):
        pass

    def schedule(self):
        # Implement a simple FIFO scheduler
        to_execute = []
        to_schedule_jobs = self.submitted_jobs()
        self.logger.info("Start scheduling jobs, nb jobs to schedule: {}".format(
            len(to_schedule_jobs)))

        self.logger.debug("jobs to be scheduled: \n{}".format(to_schedule_jobs))
        for job in to_schedule_jobs:
            if len(self.free_resources) == 0:
                break
            self.allocate_first_fit_in_best_effort(job)
            to_execute.append(job)

        self.bs.execute_jobs(to_execute)
        for job in to_execute:
            job.job_state = Job.State.RUNNING
        self.logger.info("Finished scheduling jobs, nb jobs scheduled: {}".format(
            len(to_execute)))
        self.logger.debug("jobs to be executed: \n{}".format(to_execute))

