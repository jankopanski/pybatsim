"""
    batsim.sched.job
    ~~~~~~~~~~~~~~~~

    This module provides general abstractions to manage jobs (either created by Batsim
    or by the user to submit dynamic jobs).

"""
from batsim.batsim import Job as BatsimJob, Batsim

from .utils import FilterList


class Job:
    """A job is a wrapper around a batsim job to extend the basic API of Pybatsim with more
    object oriented approaches on the implementation of the scheduler.

    :param batsim_job: the batsim job object from the underlying Pybatsim scheduler.
    """

    def __init__(self, batsim_job=None):
        self._batsim_job = batsim_job

        self._marked_for_scheduling = False
        self._scheduled = False

        self._killed = False
        self._marked_for_killing = False

        self._rejected = False
        self._marked_for_rejection = False
        self._rejected_reason = None

        self._reservation = []
        self._previous_reservations = []

    def free_all(self):
        """Free all reserved resources."""
        assert self._batsim_job

        self._previous_reservations.append(self._reservation[:])
        for res in self._reservation[:]:
            self.free(res)

    def free(self, resource, recursive_call=False):
        """Free the given `resource` object or fails if the resource is currently
        not reserved by this job.
        """
        assert self._batsim_job

        # To free resources the job does either have to be not submitted yet
        # or the job has to be completed (i.e. the job status is set by
        # batsim).
        assert (
            not self.marked_for_scheduling and not self.scheduled) or self.completed

        if not recursive_call:
            resource.free(self, recursive_call=True)
        self._reservation.remove(resource)

    def reserve(self, resource, recursive_call=False):
        """Reserves a given `resource` to ensure exclusive access (time-sharing is currently not
        implemented).
        """
        assert self._batsim_job
        if not recursive_call:
            resource.allocate(self, recursive_call=True)
        self._reservation += resource.resources

    @property
    def open(self):
        """Whether or not this job is still open."""
        return True not in [
            self.completed,
            self.marked_for_scheduling, self.scheduled,
            self.marked_for_killing, self.killed,
            self.marked_for_rejection, self.rejected]

    @property
    def completed(self):
        """Whether or not this job has been completed."""
        return self._batsim_job.status in ["SUCCESS", "TIMEOUT"]

    @property
    def marked_for_scheduling(self):
        """Whether or not this job was marked for scheduling at the end of the iteration."""
        return self._marked_for_scheduling

    @property
    def scheduled(self):
        """Whether or not this job was already submitted to Batsim for exection."""
        return self._scheduled

    def _do_execute(self, scheduler):
        """Internal method to execute the execution of the job."""
        assert self._batsim_job is not None
        assert not self.scheduled and not self.rejected

        if self.marked_for_scheduling and not self.scheduled:
            if self._batsim_job.requested_resources < len(self._reservation):
                scheduler.warn(
                    "Scheduling of job ({}) is postponed since not enough resources are allocated",
                    self)
                return

            alloc = []
            for res in self._reservation[:self._batsim_job.requested_resources]:
                alloc.append(res.id)

            scheduler._batsim.start_jobs(
                [self._batsim_job], {self._batsim_job.id: alloc})

            scheduler.info("Scheduled job ({})", self)
            self._marked_for_scheduling = False
            self._scheduled = True

    def _do_complete_job(self, scheduler):
        scheduler.info("Remove completed job and free resources: {}"
                       .format(self))
        self.free_all()

    @property
    def marked_for_rejection(self):
        """Whether or not this job will be rejected at the end of the iteration."""
        return self._marked_for_rejection

    @property
    def rejected(self):
        """Whether or not this job was submitted for rejection to batsim."""
        return self._rejected

    @property
    def rejected_reason(self):
        """The reason for the rejection"""
        return self._rejected_reason

    def reject(self, reason=""):
        """Reject the job. A reason can be given which will show up in the scheduler logs.
        However, it will currently not show up in Batsim directly as a rejecting reason is
        not part of the protocol."""
        assert self._batsim_job
        assert not self.rejected and not self.scheduled and not self.killed

        self._marked_for_rejection = True
        self._rejected_reason = reason

    def _do_reject(self, scheduler):
        """Internal method to execute the rejecting of the job."""
        if self.marked_for_rejection and not self.rejected:
            scheduler.info(
                "Rejecting job ({}), reason={}",
                self, self.rejected_reason)
            scheduler._batsim.reject_jobs([self._batsim_job])
            del scheduler._scheduler._jobmap[self._batsim_job.id]

            self._rejected = True
            self._marked_for_rejection = False

    @property
    def marked_for_killing(self):
        """Whether or not this job was marked for killing at the end of the iteration."""
        return self._marked_for_killing

    @property
    def killed(self):
        """Whether or not this job has been sent to Batsim for killing."""
        return self._killed

    def kill(self):
        """Kill the current job during its execution."""
        assert self._batsim_job
        assert not self.rejected
        self._killed = True

    def _do_kill(self, scheduler):
        """Internal method to execute the killing of the job."""
        if self.marked_for_killing and not self.killed:
            scheduler.info("Killing job ({})", self)
            scheduler._batsim.kill_jobs([self._batsim_job])

            self._killed = True
            self._marked_for_killing = True

    @property
    def reservation(self):
        """Returns the current reservation of this job."""
        return tuple(self._reservation)

    @property
    def previous_reservations(self):
        """Returns a collection of previous reservations of this job."""
        return tuple(self._previous_reservations)

    def schedule(self, resource=None):
        """Mark this job for scheduling. This can also be done even when not enough resources are
        reserved. The job will not be sent to Batsim until enough resources were reserved."""
        assert not self.rejected
        assert not self.scheduled
        assert self._batsim_job

        if resource:
            self.reserve(resource)

        self._marked_for_scheduling = True

    def change_state(self, scheduler, state, kill_reason=""):
        """Change the state of a job. This is only needed in rare cases where the real job
        should not be executed but instead the state should be set manually.
        """
        assert self._batsim_job
        scheduler._batsim.change_job_state(job, state, kill_reason)

    def __str__(self):
        return (
            "<Job {}; sub:{} reqtime:{} res:{} prof:{} fin:{} stat:{} jstat:{} kill:{}>"
            .format(
                self.id, self.submit_time, self.requested_time,
                self.requested_resources, self.profile,
                self.finish_time, self.status,
                self.job_state, self.kill_reason))

    @property
    def id(self):
        assert self._batsim_job
        return self._batsim_job.id

    @property
    def submit_time(self):
        assert self._batsim_job
        return self._batsim_job.submit_time

    @property
    def requested_time(self):
        assert self._batsim_job
        return self._batsim_job.requested_time

    @property
    def requested_resources(self):
        assert self._batsim_job
        return self._batsim_job.requested_resources

    @property
    def profile(self):
        assert self._batsim_job
        return self._batsim_job.profile

    @property
    def finish_time(self):
        assert self._batsim_job
        return self._batsim_job.finish_time

    @property
    def status(self):
        assert self._batsim_job
        return self._batsim_job.status

    @property
    def job_state(self):
        assert self._batsim_job
        return self._batsim_job.job_state

    @property
    def kill_reason(self):
        assert self._batsim_job
        return self._batsim_job.kill_reason

    @classmethod
    def create_dynamic_job(cls, *args, **kwargs):
        return DynamicJob(*args, **kwargs)

    @property
    def is_user_job(self):
        return self.id.startswith(Batsim.DYNAMIC_JOB_PREFIX + "!")


class DynamicJob(Job):
    """A DynamicJob may be used to construct dynamic jobs afterwards submitted to the scheduler.
    It has no related batsim_job since it is not known by Batsim yet. Instead it should be submitted
    and will be bounced back to the scheduler as a job known by Batsim and can be executed in this state.

    :param job_id: the id of the job (a dynamic job id will be generated, so this id is always guaranteed to be unique).

    :param requested_resources: the number of requested resources.

    :param requested_time: the number of requested time (walltime)

    :param profile: The profile object (either a `Profile` object or a dictionary containing the
    actual Batsim profile configuration).

    :param profile_name: The name of the profile to be stored in Batsim (will be dynamically generated if omitted).

    :param workload_name: The name of the workload which should be chosen if the profiles should be cached, since profiles are always related to their workload. If omitted a dynamically generated name for the workload will be used.
    """

    def __init__(
            self,
            requested_resources,
            requested_time,
            profile,
            profile_name=None,
            workload_name=None):
        super().__init__(None)
        self._user_job_id = None
        self._user_requested_resources = requested_resources
        self._user_requested_time = requested_time
        self._user_profile = profile
        self._user_profile_name = profile_name
        self._user_workload_name = workload_name

        self._dyn_marked_submission = False
        self._dyn_submitted = False

    @property
    def id(self):
        return self._user_job_id

    @property
    def submit_time(self):
        return None

    @property
    def requested_time(self):
        return self._user_requested_time

    @property
    def requested_resources(self):
        return self._user_requested_resources

    @property
    def profile(self):
        return self._user_profile_name

    @property
    def profile_object(self):
        return self._user_profile

    @property
    def workload_name(self):
        return self._user_workload_name

    @property
    def finish_time(self):
        return None

    @property
    def status(self):
        return None

    @property
    def job_state(self):
        return BatsimJob.State.UNKNOWN

    @property
    def kill_reason(self):
        return None

    def submit(self, scheduler):
        """Marks a dynamic job for submission in the `scheduler`."""
        if not self._dyn_marked_submission and not self._dyn_submitted:
            self._dyn_marked_submission = True
            scheduler.jobs.append(self)

    @property
    def marked_for_dynamic_submission(self):
        """Whether or not this job object was marked for dynamic submission."""
        return self._dyn_marked_submission

    @property
    def completed(self):
        return False

    @property
    def dynamically_submitted(self):
        """Whether or not this job object was dynamically submitted."""
        return self._dyn_submitted

    def _do_dyn_submit(self, scheduler):
        """Execute the dynamic job submission in the `scheduler`."""
        if self._dyn_marked_submission and not self._dyn_submitted:
            # The profile object will be executed if it is no dictionary already to
            # allow complex Profile objects.
            profile = self._user_profile
            if not isinstance(profile, dict):
                profile = profile()

            scheduler.info("Submit dynamic job ({})", self)
            self._user_job_id = scheduler._batsim.submit_job(
                self._user_requested_resources,
                self._user_requested_time,
                profile,
                self._user_profile_name,
                self._user_workload_name)

            self._dyn_submitted = True
            self._dyn_marked_submission = False


class Jobs(FilterList):
    """Helper class implementing parts of the python list API to manage the jobs.

       :param from_list: a list of `Job` objects to be managed by this wrapper.
    """

    @property
    def open(self):
        return self.filter(open=True)

    @property
    def completed(self):
        return self.filter(completed=True)

    @property
    def rejected(self):
        return self.filter(rejected=True)

    @property
    def marked_for_rejection(self):
        return self.filter(marked_for_rejection=True)

    @property
    def scheduled(self):
        return self.filter(scheduled=True)

    @property
    def marked_for_scheduling(self):
        return self.filter(marked_for_scheduling=True)

    @property
    def killed(self):
        return self.filter(killed=True)

    @property
    def marked_for_killing(self):
        return self.filter(marked_for_killing=True)

    @property
    def dynamically_submitted(self):
        return self.filter(dynamically_submitted=True)

    @property
    def marked_for_dynamic_submission(self):
        return self.filter(marked_for_dyanmic_submission=True)

    def filter(
            self,
            cond=None,
            open=False,
            completed=False,
            rejected=False,
            marked_for_rejection=False,
            scheduled=False,
            marked_for_scheduling=False,
            killed=False,
            marked_for_killing=False,
            dynamically_submitted=False,
            marked_for_dynamic_submission=False,
            limit=None,
            min=None,
            num=None):
        """Filter the jobs lists to search for jobs.

        :param cond: a function evaluating the current resource and returns True or False whether or not the resource should be returned.

        :param open: whether the job is still open.

        :param completed: whether the job has already been completed.

        :param rejected: whether the job has already been rejected.

        :param marked_for_rejection: whether the job has been marked for rejection.

        :param scheduled: whether the job has already been scheduled.

        :param marked_for_scheduling: whether the job has been marked for scheduling.

        :param killed: whether the job has been sent for killing.

        :param marked_for_killing: whether the job has been marked for killing.

        :param dynamically_submitted: whether the job has been submited as dynamic job.

        :param marked_for_dynamic_submission: whether the job has been marked for dynamic job submission.

        :param limit: the maximum number of returned jobs.

        :param min: the minimum number of returned jobs (if less jobs are available no jobs will be returned at all).

        :param num: the exact number of returned jobs.
        """

        # Yield all jobs if not filtered
        if True not in [
                open, completed,
                rejected, marked_for_rejection,
                scheduled, marked_for_scheduling,
                killed, marked_for_killing,
                dynamically_submitted, marked_for_dynamic_submission]:
            open = True
            completed = True

            rejected = True
            marked_for_rejection = True

            scheduled = True
            marked_for_scheduling = True

            killed = True
            marked_for_killing = True

            dynamically_submitted = True
            marked_for_dynamic_submission = True

        # Filter jobs
        def filter_jobs(jobs):
            for j in jobs:
                if j.completed:
                    if completed:
                        yield j
                elif j.rejected:
                    if rejected:
                        yield j
                elif j.marked_for_rejection:
                    if marked_for_rejection:
                        yield j
                elif j.scheduled:
                    if scheduled:
                        yield j
                elif j.marked_for_scheduling:
                    if marked_for_scheduling:
                        yield j
                elif j.killed:
                    if killed:
                        yield j
                elif j.marked_for_killing:
                    if marked_for_killing:
                        yield j
                elif isinstance(j, DynamicJob):
                    if j.dynamically_submitted:
                        if dynamically_submitted:
                            yield j
                    elif j.marked_for_dynamic_submission:
                        if marked_for_dynamic_submission:
                            yield j
                else:  # open job
                    if open:
                        yield j

        return self.base_filter([filter_jobs], cond, limit, min, num)
