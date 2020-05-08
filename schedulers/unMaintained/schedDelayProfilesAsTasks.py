"""
    schedDelayProfilesAsTasks
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    This is a small scheduler to test the dynamic API of Batsim.
    Delay jobs are split into small tasks and submitted individually to Batsim.

    This is only a simple prototype and many cases might not be handled, e.g. the walltime
    of the parent job is not checked and the tasks just run in their own single
    walltime. However, it shows how jobs can be split into smaller jobs and still achieve
    the almost same makespan.
"""
from batsim.sched import Job
from batsim.sched import Scheduler
from batsim.sched import Allocation
from batsim.sched import Profiles

from batsim.sched.algorithms.utils import consecutive_resources_filter


class SchedDelayProfilesAsTasks(Scheduler):

    def on_job_submission(self, job):
        # Do this only on static job submissions
        if not job.is_dynamic_job:

            # Reject jobs which are too big
            if job.requested_resources > len(self.resources):
                job.reject(
                    "Too few resources available in the system (overall)")
            elif job.profile.type == "delay":
                max_delay = int(job.profile.delay)

                # Split the delay of the job into smaller tasks
                tasks = []
                for i in range(0, max_delay, 5):
                    next_val = i + 5
                    if next_val > max_delay:
                        next_val = max_delay
                    length = next_val - i
                    tasks.append((i, length))

                # Calculate the walltime for the individual tasks.
                # Increase the walltime by a small amount since otherwise it
                # could happen that Batsim will kill the individual tasks.
                walltimes_each = job.requested_time / len(tasks) + 0.00001

                # Create all sub jobs and submit them to Batsim
                for task in tasks:
                    job.submit_sub_job(job.requested_resources,
                                       walltimes_each,
                                       Profiles.Delay(task[1]))
            # This scheduler only handles delay jobs
            else:
                self.fatal(
                    "Non-delay jobs are not supported with this prototype scheduler")

    def schedule(self):
        # For all static jobs which are runnable search the first sub job which
        # is runnable and start it.
        for job in self.jobs.static_job.runnable:
            for sj in job.sub_jobs.runnable:
                start_time, res = self.resources.find_with_earliest_start_time(
                    job, filter=consecutive_resources_filter)

                # If no resources are found this time, it will be tried again at
                # the next scheduler iteration (i.e. when another job has
                # finished and the resources are free again)
                if res:
                    sj.schedule(res)

                    # The remaining allocation is a reservation for the
                    # remaining part of the static job (the remaining tasks).
                    job.remaining_allocation = Allocation(
                        start_time + sj.requested_time + 1, job.requested_time -
                        sj.requested_time, res)

                    # Set the job state of the static job to running because we
                    # now have a sub job working on the parent job's task.
                    job.change_state(Job.State.RUNNING)
                break

    def on_job_completion(self, job):
        # Do this only for completed dynamic jobs
        if job.is_dynamic_job:
            scheduled = False

            if job.success:
                # Search for the first sub job which is runnable (i.e. an another
                # sub job of the same parent job as the job which has
                # completed)
                for j in job.parent_job.sub_jobs.runnable:
                    scheduled = True

                    # Collect information about the remaining allocation
                    alloc = job.parent_job.remaining_allocation
                    res = alloc.resources.copy()
                    starttime = alloc.start_time
                    walltime = alloc.walltime
                    alloc.remove_all_resources()

                    # Schedule the sub job. Afterwards create a new reservation for
                    # the remaining part of the parent job's walltime.
                    j.schedule(res)
                    if walltime - j.requested_time > 0:
                        job.parent_job.remaining_allocation = Allocation(
                            self.time + j.requested_time + 1, walltime - j.requested_time, res)
                    break

                # This was the last sub job of its parent if nothing was scheduled.
                # Set the state to completed in Batsim.
                if not scheduled:
                    job.parent_job.remaining_allocation.remove_all_resources()
                    job.parent_job.change_state(
                        Job.State.COMPLETED_SUCCESSFULLY)
            else:
                # Reject the other sub jobs which should be removed from the
                # queue.
                for j in job.parent_job.sub_jobs.open:
                    j.reject(self)

                # Set the state of the parent job
                job.parent_job.remaining_allocation.remove_all_resources()
                job.parent_job.change_state(job.state)
