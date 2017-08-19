"""
    batsim.sched.job
    ~~~~~~~~~~~~~~~~

    This module provides general abstractions to manage jobs (either created by Batsim
    or by the user to submit dynamic jobs).

"""
from batsim.batsim import Job as BatsimJob


class Job:
    """A job is a wrapper around a batsim job to extend the basic API of Pybatsim with more
    object oriented approaches on the implementation of the scheduler.

    :param batsim_job: the batsim job object from the underlying Pybatsim scheduler.
    """

    def __init__(self, batsim_job=None):
        self._batsim_job = batsim_job
        self._scheduled = False
        self._submitted = False
        self._killed = False
        self._rejected = False
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
        assert not self._submitted or self._batsim_job.status in [
            "SUCCESS", "TIMEOUT"]

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
    def scheduled(self):
        """Whether or not this job was marked for scheduling at the end of the iteration."""
        return self._scheduled

    @property
    def rejected(self):
        """Whether or not this job will be rejected at the end of the iteration."""
        return self._rejected

    @property
    def reservation(self):
        """Returns the current reservation of this job."""
        return tuple(self._reservation)

    @property
    def submitted(self):
        """Whether or not this job was already submitted to Batsim for exection."""
        return self._submitted

    @property
    def killed(self):
        """Whether or not this job was marked for killing at the end of the iteration."""
        return self._killed

    @property
    def previous_reservations(self):
        """Returns a collection of previous reservations of this job."""
        return tuple(self._previous_reservations)

    def schedule(self, resource=None):
        """Mark this job for scheduling. This can also be done even when not enough resources are
        reserved. The job will not be sent to Batsim until enough resources were reserved."""
        assert not self.rejected
        assert self._batsim_job

        if resource:
            self.reserve(resource)

        assert not self.submitted
        self._scheduled = True

    def reject(self, reason=""):
        """Reject the job. A reason can be given which will show up in the scheduler logs.
        However, it will currently not show up in Batsim directly as a rejecting reason is
        not part of the protocol."""
        assert self._batsim_job
        assert not self.submitted and not self.scheduled
        self._rejected = True
        self._rejected_reason = reason

    def change_state(self, scheduler, state, kill_reason=""):
        """Change the state of a job. This is only needed in rare cases where the real job
        should not be executed but instead the state should be set manually.
        """
        assert self._batsim_job
        scheduler._batsim.change_job_state(job, state, kill_reason)

    def kill(self):
        """Kill the current job during its execution."""
        assert self._batsim_job
        assert not self.rejected
        self._killed = True

    def _do_kill(self, scheduler):
        """Internal method to execute the killing of the job."""
        scheduler.info("Killing job ({})", self)
        scheduler._batsim.kill_jobs([self._batsim_job])

    def _do_reject(self, scheduler):
        """Internal method to execute the rejecting of the job."""
        scheduler.info(
            "Rejecting job ({}), reason={}",
            self, self._rejected_reason)
        scheduler._batsim.reject_jobs([self._batsim_job])
        scheduler._open_jobs.remove(self)
        scheduler._rejected_jobs.append(self)
        del scheduler._job_map[self._batsim_job.id]

    def _do_execute(self, scheduler):
        """Internal method to execute the execution of the job."""
        assert self._batsim_job is not None
        assert not self.submitted and not self.rejected

        if not self.scheduled:
            self.schedule()

        if self._batsim_job.requested_resources < len(self._reservation):
            scheduler.warn(
                "Starting of job ({}) is postponed since not enough resources are allocated",
                self)
            return

        self._submitted = True

        alloc = []
        for res in self._reservation[:self._batsim_job.requested_resources]:
            alloc.append(res.id)

        scheduler._batsim.start_jobs(
            [self._batsim_job], {self._batsim_job.id: alloc})

        self._scheduled = False

        scheduler._open_jobs.remove(self)
        scheduler._scheduled_jobs.append(self)

        scheduler.info("Starting job ({})", self)

    def _complete_job(self, scheduler):
        scheduler.info("Remove completed job and free resources: {}"
                       .format(self))
        self.free_all()
        scheduler._scheduled_jobs.remove(self)
        scheduler._completed_jobs.append(self)
        scheduler._new_completed_jobs.append(self)
        del scheduler._job_map[self._batsim_job.id]

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
    def create(cls, *args, **kwargs):
        return UserJob(*args, **kwargs)


class UserJob(Job):
    """A UserJob may be used to construct dynamic jobs afterwards submitted to the scheduler.
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
            job_id,
            requested_resources,
            requested_time,
            profile,
            profile_name=None,
            workload_name=None):
        super().__init__(None)
        self._user_job_id = job_id
        self._user_requested_resources = requested_resources
        self._user_requested_time = requested_time
        self._user_profile = profile
        self._user_profile_name = profile_name
        self._user_workload_name = workload_name

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
        self._dyn_submitted = True
        scheduler._dyn_jobs.append(self)

    @property
    def dynamically_submitted(self):
        """Whether or not this job object was marked for dynamic submission."""
        return self._dyn_submitted

    def _do_dyn_submit(self, scheduler):
        """Execute the dynamic job submission in the `scheduler`."""
        # The profile object will be executed if it is no dictionary already to
        # allow complex Profile objects.
        profile = self._user_profile
        if not isinstance(profile, dict):
            profile = profile()

        scheduler.info("Submit dynamic job ({})", self)
        scheduler._batsim.submit_job(
            self._user_job_id,
            self._user_requested_resources,
            self._user_requested_time,
            profile,
            self._user_profile_name,
            self._user_workload_name)
        scheduler._dyn_jobs.remove(self)
