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

The Batsim job profile "msg_hg_tot" or a sequence of that kind of jobs are
MANDATORY for this mechanism to work.

Also, the folowing batsim configuration is mandatory:
```json
{
    "job_submission": {
        "forward_profiles": false,
        "from_scheduler": {
          "enabled": false,
          "acknowledge": true
        }
    },
    "job_kill": {
        "forward_profiles": false
    }
}
```
"""

from batsim.batsim import BatsimScheduler, Job

from procset import ProcSet, ProcInt
import logging
import copy

def sort_by_id(jobs):
    return sorted(jobs, key=lambda j: int(j.id.split('!')[1].split('#')[0]))


class SchedBebida(BatsimScheduler):

    def filter_jobs_by_state(self, state):
        return sort_by_id([job for job in self.bs.jobs.values() if
            job.job_state == state])

    def running_jobs(self):
        return self.filter_jobs_by_state(Job.State.RUNNING)

    def submitted_jobs(self):
        return self.filter_jobs_by_state(Job.State.SUBMITTED)

    def in_killing_jobs(self):
        return self.filter_jobs_by_state(Job.State.IN_KILLING)

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

        iter_intervals = (self.free_resources & self.available_resources).intervals()
        for curr_interval in iter_intervals:
            if (len(allocation) >= job.requested_resources):
                break
            #import ipdb; ipdb.set_trace()
            interval_size = len(curr_interval)
            self.logger.debug("Interval lookup: {}".format(curr_interval))

            if interval_size > nb_resources_still_needed:
                allocation.insert(
                    ProcInt(
                        inf=curr_interval.inf,
                        sup=(curr_interval.inf + nb_resources_still_needed -1))
                )
            elif interval_size == nb_resources_still_needed:
                allocation.insert(copy.deepcopy(curr_interval))
            elif interval_size < nb_resources_still_needed:
                allocation.insert(copy.deepcopy(curr_interval))
                nb_resources_still_needed = nb_resources_still_needed - interval_size

        if len(allocation) > 0:
            job.allocation = allocation
            job.state = Job.State.RUNNING

            # udate free resources
            self.free_resources = self.free_resources - job.allocation

            self.logger.info("Allocation for job {}: {}".format(
                job.id, job.allocation))


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.to_be_removed_resources = {}
        self.load_balanced_jobs = set()

    def onSimulationBegins(self):
        self.free_resources = ProcSet(*[res_id for res_id in
            self.bs.resources.keys()])
        self.nb_total_resources = len(self.free_resources)
        self.available_resources = copy.deepcopy(self.free_resources)

        assert self.bs.batconf["job_submission"]["forward_profiles"] == True, (
                "Forward profile is mandatory for resubmit to work")

    def onJobSubmission(self, job):
        assert "type" in job.profile_dict, "Forward profile is mandatory"
        assert (job.profile_dict["type"] == "msg_par_hg_tot" or
                job.profile_dict["type"] == "composed")

    def onJobCompletion(self, job):
        # If it is a job killed, resources where already where already removed
        # and we don't want other jobs to use these resources.
        # But, some resources of the allocation are not part of the removed
        # resources: we have to make it available
        if job.job_state == Job.State.COMPLETED_KILLED:
            to_add_resources = job.allocation & self.available_resources
            self.logger.debug("To add resources: {}".format(to_add_resources))
            self.free_resources = self.free_resources | to_add_resources
        else:
            # udate free resources
            self.free_resources = self.free_resources | job.allocation
            self.load_balance_jobs()

    def onNoMoreEvents(self):
        if len(self.free_resources) > 0:
            self.schedule()

        self.logger.debug("=====================NO MORE EVENTS======================")
        self.logger.debug("\nFREE RESOURCES = {}".format(str(self.free_resources)))
        self.logger.debug("\nAVAILABLE RESOURCES = {}".format(str(self.available_resources)))
        self.logger.debug("\nTO BE REMOVED RESOURCES: {}".format(str(self.to_be_removed_resources)))
        nb_used_resources = self.nb_total_resources - len(self.free_resources)
        nb_allocated_resources = sum([len(job.allocation) for job in
            self.running_jobs()])
        self.logger.debug(("\nNB USED RESOURCES = {}").format(nb_used_resources))


        self.logger.debug(("\nSUBMITTED JOBS = {}\n"
                           "SCHEDULED JOBS = {}\n"
                           "COMPLETED JOBS = {}"
                           ).format(
                               self.bs.nb_jobs_received,
                               self.bs.nb_jobs_scheduled,
                               self.bs.nb_jobs_completed,
                               ))
        self.logger.debug("\nJOBS: \n{}".format(self.bs.jobs))

    def onRemoveResources(self, resources):
        self.available_resources = self.available_resources - ProcSet.from_str(resources)

        # find the list of jobs that are impacted
        # and kill all those jobs
        to_be_killed = []
        for job in self.running_jobs():
            if job.allocation & ProcSet.from_str(resources):
                to_be_killed.append(job)

        if len(to_be_killed) > 0:
            self.bs.kill_jobs(to_be_killed)

        # check that no job in Killing are still allocated to this resources
        # because some jobs can be already in killing before this function call
        self.logger.debug("Jobs that are in killing: {}".format(self.in_killing_jobs()))
        in_killing = self.in_killing_jobs()
        if not in_killing or all([len(job.allocation & ProcSet.from_str(resources)) == 0
                                for job in in_killing]):
            # notify resources removed now
            self.bs.notify_resources_removed(resources)
        else:
            # keep track of resources to be removed that are from killed jobs
            # related to a previous event
            self.to_be_removed_resources[resources] = [
                    job for job in
                    in_killing if len(job.allocation &
                        ProcSet.from_str(resources)) != 0]

    def onAddResources(self, resources):
        self.available_resources = self.available_resources | ProcSet.from_str(resources)
        # add the resources
        self.free_resources = self.free_resources | ProcSet.from_str(resources)

        self.load_balance_jobs()

    def load_balance_jobs(self):
        """
        find the list of jobs that need more resources
        kill jobs, so tey will be resubmited taking free resources, until
        tere is no more resources
        """
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
            # mark those jobs in order to resubmit them without penalty
            self.load_balanced_jobs.update({job.id for job in to_be_killed})

    def onJobsKilled(self, jobs):
        # First notify that the resources are removed
        to_remove = []
        for resources, to_be_killed in self.to_be_removed_resources.items():
            if (len(to_be_killed) > 0
                and any([job in jobs for job in to_be_killed])):
                # Notify that the resources was removed
                self.bs.notify_resources_removed(resources)
                to_remove.append(resources)
                # Mark the resources as not available
                self.free_resources = self.free_resources - ProcSet.from_str(resources)
        # Clean structure
        for resources in to_remove:
            del self.to_be_removed_resources[resources]

        # get killed jobs progress and resubmit what's left of the jobs
        for old_job in jobs:
            progress = old_job.progress
            if "current_task_index" not in progress:
                new_job = old_job
            else:
                # WARNING only work for simple sequence job without sub sequence
                curr_task = progress["current_task_index"]
                # get profile to resubmit current and following sequential
                # tasks
                new_job_seq_size = len(old_job.profile_dict["seq"][curr_task:])
                old_job_seq_size = len(old_job.profile_dict["seq"])

                self.logger.debug("Job {} resubmitted stages: {} out of {}".format(
                        old_job.id,
                        new_job_seq_size,
                        old_job_seq_size))

                if old_job.id in self.load_balanced_jobs:
                    # clean the set
                    self.load_balanced_jobs.remove(old_job.id)

                    # Create a new job with a profile that corespond to the work that left
                    new_job = copy.deepcopy(old_job)
                    curr_task_progress = progress["current_task"]["progress"]
                    new_job.profile = old_job.profile + "#" + str(curr_task) + "#" + str(curr_task_progress)
                    new_job.profile_dict["seq"] = old_job.profile_dict["seq"][curr_task:]

                    # Now let's modify the current profile to reflect progress
                    assert "profile" in progress["current_task"], ('The profile'
                            ' is not forwarded in the job progress: set'
                            ' {"job_kill": {"forward_profiles": true}} in the '
                            'batsim config')
                    curr_task_profile = progress["current_task"]["profile"]
                    assert curr_task_profile["type"] == "msg_par_hg_tot", "Only msg_par_hg_tot profile are supported right now"
                    for key, value in curr_task_profile.items():
                        if isinstance(value, (int, float)):
                            curr_task_profile[key] = value * (1 - curr_task_progress)
                    parent_task_profile = progress["current_task"]["profile_name"].split("#")[0]
                    curr_task_profile_name =  parent_task_profile + "#" + str(curr_task_progress)


                    new_job.profile_dict["seq"][0] = curr_task_profile_name

                    # submit the new internal current task profile
                    self.bs.submit_profiles(
                            new_job.id.split("!")[0],
                            {curr_task_profile_name: curr_task_profile})

                elif (new_job_seq_size == old_job_seq_size):
                    # no modification to do: resubmit the same job
                    new_job = old_job
                else:
                    # create a new profile: remove already finished stages
                    new_job = copy.deepcopy(old_job)
                    new_job.profile = old_job.profile + "#" + str(curr_task)
                    new_job.profile_dict["seq"] = old_job.profile_dict["seq"][curr_task:]

            # Re-submit the profile
            self.bs.resubmit_job(new_job)

    def onDeadlock(self):
        pass

    def schedule(self):
        # Implement a simple FIFO scheduler
        if len(self.free_resources & self.available_resources) == 0:
            return
        to_execute = []
        to_schedule_jobs = self.submitted_jobs()
        self.logger.info("Start scheduling jobs, nb jobs to schedule: {}".format(
            len(to_schedule_jobs)))

        self.logger.debug("jobs to be scheduled: \n{}".format(to_schedule_jobs))
        for job in to_schedule_jobs:
            if len(self.free_resources & self.available_resources) == 0:
                break
            self.allocate_first_fit_in_best_effort(job)
            to_execute.append(job)

        self.bs.execute_jobs(to_execute)
        for job in to_execute:
            job.job_state = Job.State.RUNNING
        self.logger.info("Finished scheduling jobs, nb jobs scheduled: {}".format(
            len(to_execute)))
        self.logger.debug("jobs to be executed: \n{}".format(to_execute))

