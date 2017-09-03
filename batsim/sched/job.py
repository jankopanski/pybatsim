"""
    batsim.sched.job
    ~~~~~~~~~~~~~~~~

    This module provides general abstractions to manage jobs (either created by Batsim
    or by the user to submit dynamic jobs).

"""
from batsim.batsim import Job as BatsimJob, Batsim

from .utils import ObserveList, filter_list, ListView
from .alloc import Allocation
from .messages import MessageBuffer
from .profiles import Profiles


class Job:
    """A job is a wrapper around a batsim job to extend the basic API of Pybatsim with more
    object oriented approaches on the implementation of the scheduler.

    :param batsim_job: the batsim job object from the underlying Pybatsim scheduler.

    :param scheduler: the associated scheduler managing this job.

    :param job_list: the main job list where this job is contained

    :param parent_job: the parent job if this is a sub job
    """

    State = BatsimJob.State

    def __init__(
            self,
            batsim_job=None,
            scheduler=None,
            jobs_list=None,
            parent_job=None):
        self._jobs_list = jobs_list

        self._changed_state = None

        self._scheduler = scheduler
        self._batsim_job = batsim_job
        self._parent_job = parent_job

        self._scheduled = False

        self._killed = False

        self._submitted = self._batsim_job is not None

        self._rejected = False
        self._rejected_reason = None

        self._sub_jobs = []

        self._own_dependencies = []

        self._allocation = None
        self._start_time = None

        self._messages = MessageBuffer()

        self._profile = None

    def __setattr__(self, field, value):
        object.__setattr__(self, field, value)
        try:
            self._job_list.update_element(self)
        except AttributeError:
            pass

    def get_job_data(self, key, default=None):
        """Get data from the dictionary of the underlying Batsim job.

        :param key: the key to search in the underlying job dictionary

        :param default: the default value if the key is missing
        """
        try:
            return self._batsim_job.json_dict[key]
        except KeyError:
            try:
                return self._batsim_job.__dict__[key]
            except KeyError:
                return default

    @property
    def messages(self):
        """The buffer of incoming messages"""
        return self._messages

    def send(self, message):
        """Send a message to a running job (assuming that this job will try to receive
        a message at any time in the future of its execution).

        :param message: the message to send to the job
        """
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        assert self.running, "Job is not running"
        self._scheduler.info(
            "Send message ({message}) to job ({job})",
            job=self,
            message=message,
            type="send_message_to_job")
        self._scheduler._batsim.send_message_to_job(self._batsim_job, message)

    @property
    def start_time(self):
        """The starting time of this job."""
        return self._start_time

    @property
    def dependencies(self):
        """The dependencies of this job.

        They are either given in the workload through the custom `deps` field of a job
        or added by the scheduler as manual dependencies.
        """
        return ListView(
            self.get_job_data("deps", []) +
            self._own_dependencies)

    @property
    def open(self):
        """Whether or not this job is still open."""
        return True not in [
            self.completed,
            self.scheduled,
            self.killed,
            self.rejected] \
            and self._changed_state is None

    @property
    def runnable(self):
        """Whether the job is still open and has only fulfilled dependencies."""
        return self.dependencies_fulfilled and self.open

    @property
    def parent_job(self):
        """The parent job of this job. This is relevant if a sub job is dynamically
        created and executed by the scheduler.
        """
        return self._parent_job

    @property
    def sub_jobs(self):
        """The sub jobs of this job.

        Sub jobs cannot be added manually and instead have to be submitted as dynamic sub
        jobs which are then added automatically.
        """
        return Jobs(self._sub_jobs)

    @property
    def running(self):
        """Whether or not this job is currently running."""
        return self.scheduled and not self.completed and not self.killed

    @property
    def completed(self):
        """Whether or not this job has been completed."""
        completed_states = set([
            BatsimJob.State.COMPLETED_KILLED,
            BatsimJob.State.COMPLETED_SUCCESSFULLY,
            BatsimJob.State.COMPLETED_FAILED])
        return self._changed_state in completed_states or self._batsim_job.job_state in completed_states

    @property
    def scheduled(self):
        """Whether or not this job was already submitted to Batsim for exection."""
        return self._scheduled

    @property
    def rejected(self):
        """Whether or not this job was submitted for rejection to batsim."""
        return self._rejected

    @property
    def submitted(self):
        """Whether or not this job was submitted to Batsim."""
        return self._submitted

    @property
    def is_dynamic_submission_request(self):
        """Whether or not this job is a request sent for dynamic submission.

        To check whether this job was originally dynamically submitted use the property:
        `is_dynamic_job` instead.
        """
        return False

    @property
    def rejected_reason(self):
        """The reason for the rejection"""
        return self._rejected_reason

    @property
    def killed(self):
        """Whether or not this job has been sent to Batsim for killing."""
        return self._killed

    @property
    def running(self):
        """Whether or not this job is currently running."""
        return self._changed_state == BatsimJob.State.RUNNING or (
            not self.open and self.scheduled and not self.completed)

    @property
    def allocation(self):
        """Returns the current allocation of this job."""
        return self._allocation

    @property
    def id(self):
        """The id of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.id

    @property
    def submit_time(self):
        """The time of submission of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.submit_time

    @property
    def requested_time(self):
        """The requested time of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.requested_time

    @property
    def requested_resources(self):
        """The requested resources of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.requested_resources

    @property
    def profile(self):
        """The profile of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        if self._profile is None:
            self._profile = Profiles.profile_from_dict(
                self._batsim_job.profile_dict, name=self._batsim_job.profile)
        return self._profile

    @property
    def finish_time(self):
        """The finish time of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.finish_time

    @property
    def state(self):
        """The state of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.job_state

    @property
    def kill_reason(self):
        """The kill reason (if any exists) of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.kill_reason

    @property
    def return_code(self):
        """The return code of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.return_code

    @property
    def is_dynamic_job(self):
        """Whether or not this job is a dynamic job."""
        return self.id.startswith(Batsim.DYNAMIC_JOB_PREFIX + "!")

    @property
    def dependencies_fulfilled(self):
        """Whether or not all dependencies of this job are fulfilled.

        :param jobs: a list of all jobs in the system which is needed to resolve
        the actual jobs to which the dependencies are referring
        """
        for dep in self.resolved_dependencies:
            if not isinstance(dep, Job) or not dep.completed:
                return False
        return True

    @property
    def resolved_dependencies(self):
        """Resolve the dependencies of this job (converting job ids to concrete job objects)."""
        jobparts = self.id.split("!")
        job_id = jobparts[-1]
        workload_name = "!".join(jobparts[:len(jobparts) - 1])
        result = []
        for dep in self.dependencies:
            # If the workload is missing: assume that the dependency refers
            # to the same workload.
            if "!" not in dep:
                dep = str(workload_name) + "!" + str(dep)
            try:
                dep_job = self._scheduler.jobs[dep]
                result.append(dep_job)
            except KeyError:
                result.append(dep)
        return ListView(result)

    def free(self):
        """Free the current allocation of this job."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        assert self._allocation is not None, "Job has no allocation"

        # To free resources the job does either have to be not submitted yet
        # or the job has to be completed (i.e. the job status is set by
        # batsim).
        assert not self.scheduled or self.completed, \
            "Job is in invalid state: not completed yet or currently scheduled"

        if self.completed:
            self._allocation._free_job_from_allocation()
        else:
            self._allocation.free()
        self._jobs_list.update_element(self)

    def reserve(self, resource):
        """Reserves a given `resource` to ensure exclusive access.

        If a resources object is given and not an allocation object, then the
        allocation will be valid for exactly the time of the job walltime. As a
        consequence, if reservations with Resource or Resources objects are made
        the jobs should be immediately scheduled afterwards because otherwise the
        allocation will have too few walltime available to fit the job.

        As an alternative an Allocation can be created manually (with a longer
        walltime) and then be given as parameter to the `reserve(resource)` method.
        In this case the times of the allocation are not touched as long as the job
        fits in the walltime.

        :param resource: either a single `Resource` a list in the form of a `Resources`
        object or an `Allocation`
        """
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        assert self._allocation is None, "Job has already an allocation"
        assert self.open, "Job is not open"

        if isinstance(resource, Allocation):
            resource._reserve_job_on_allocation(self)
            self._allocation = resource
        else:
            self._allocation = Allocation(
                start_time=self._scheduler.time,
                job=self,
                resources=resource,
                walltime=self.requested_time)
        self._jobs_list.update_element(self)

    def fits_in_frame(self, remaining_time, num_resources):
        """Determines if the job fits in the specified frame of time and resources.

        :param remaining_time: the remaining time in the frame

        :param num_resources: the number of available resources
        """
        return remaining_time >= self.requested_time and num_resources >= self.requested_resources

    def add_dependency(self, job):
        """Adds a dependency to this job.

        :param job: the job which should be added as a dependency
        """
        assert self.open, "Job is not open"
        self._own_dependencies.append(job)
        self._jobs_list.update_element(self)

    def remove_dependency(self, job):
        """Removes a dependency from this job. Jobs which are defined in the workload
        definition can not be removed.

        :param job: the job which should be removed as a dependency
        """
        assert self.open, "Job is not open"
        self._own_dependencies.remove(job)
        self._jobs_list.update_element(self)

    def _do_complete_job(self):
        """Complete a job."""
        self._scheduler.info("Remove completed job and free resources: {job}",
                             job=self, type="job_completed")
        self.allocation.free()
        self._jobs_list.update_element(self)

    def move_properties_from(self, otherjob):
        """Move properties from one job to another.

        This is used internally to convert dynamic jobs to submitted jobs."""
        parent_job = otherjob.parent_job
        if parent_job:
            i = parent_job._sub_jobs.index(otherjob)
            parent_job._sub_jobs[i] = self
            self._parent_job = parent_job
        self._own_dependencies = list(otherjob._own_dependencies)
        self._jobs_list.update_element(self)

    def reject(self, reason=""):
        """Reject the job. A reason can be given which will show up in the scheduler logs.
        However, it will currently not show up in Batsim directly as a rejecting reason is
        not part of the protocol.

        :param reason: the reason for the job rejection
        """
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        assert self.open, "Job is not open"
        assert not self.rejected, "Job is alrady rejected"

        self._rejected_reason = reason

        self._scheduler.info(
            "Rejecting job ({job}), reason={reason}",
            job=self, reason=self.rejected_reason, type="job_rejection")
        self._scheduler._batsim.reject_jobs([self._batsim_job])
        del self._scheduler._scheduler._jobmap[self._batsim_job.id]

        self._rejected = True
        self._jobs_list.update_element(self)

    def kill(self):
        """Kill the current job during its execution."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        assert self.running, "Job is not running"
        assert not self.killed, "Job is already killed"
        assert not self.open, "Job is still open"
        assert self.running, "Job is not currently not running"

        self._scheduler.info(
            "Killing job ({job})",
            job=self,
            type="job_killing")
        self._scheduler._batsim.kill_jobs([self._batsim_job])

        self._killed = True
        self._jobs_list.update_element(self)

    def schedule(self, resource=None):
        """Mark this job for scheduling. This can also be done even when not enough resources are
        reserved. The job will not be sent to Batsim until enough resources were reserved."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        assert self.open, "Job is not open"
        assert not self.scheduled and not self.rejected, "Job is either already scheduled or rejected"

        if resource:
            self.reserve(resource)

        assert self.allocation is not None, "Job has no allocation"

        if self._batsim_job.requested_resources < len(self.allocation):
            self._scheduler.warn(
                "Scheduling of job ({job}) is postponed since not enough resources are allocated",
                job=self, type="job_starting_postponed_too_few_resources")
            return

        if not self._scheduler.has_time_sharing:
            for r in self.allocation.resources:
                for a1 in r.allocations:
                    for a2 in r.allocations:
                        if a1 != a2:
                            if a1.overlaps_with(a2):
                                raise ValueError(
                                    "Time sharing is not enabled in Batsim (resource allocations are overlapping)")

        if not self.allocation.fits_job_for_remaining_time(self):
            raise ValueError(
                "Job does not fit in the remaining time frame of the allocation")

        # Abort job start if allocation is in the future
        if self.allocation.start_time > self._scheduler.time:
            self._scheduler.run_scheduler_at(self.allocation.start_time)
            return

        # If the allocation is larger than required only choose as many resources
        # as necessary.
        r = range(0, self.requested_resources)
        self.allocation.allocate(self._scheduler, r)

        alloc = []
        for res in self.allocation.allocated_resources:
            alloc.append(res.id)

        # Start the jobs
        self._scheduler._batsim.start_jobs(
            [self._batsim_job], {self.id: alloc})

        self._scheduler.info(
            "Scheduled job ({job})",
            job=self,
            type="job_scheduled")
        self._scheduled = True
        self._start_time = self._scheduler.time
        self._jobs_list.update_element(self)

    def change_state(self, state, kill_reason=""):
        """Change the state of a job. This is only needed in rare cases where the real job
        should not be executed but instead the state should be set manually.
        """
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        self._scheduler._batsim.change_job_state(
            self._batsim_job, state, kill_reason)
        self._changed_state = state
        self._jobs_list.update_element(self)

    def __str__(self):
        return (
            "<Job {}; sub:{} reqtime:{} res:{} prof:{} fin:{} stat:{} killreason:{} ret:{}>"
            .format(
                self.id, self.submit_time, self.requested_time,
                self.requested_resources, self.profile,
                self.finish_time, self.state,
                self.kill_reason,
                self.return_code))

    @classmethod
    def create_dynamic_job(cls, *args, **kwargs):
        """Create a dynamic job.

        :param requested_resources: the number of requested resources.

        :param requested_time: the number of requested time (walltime)

        :param profile: The profile object (either a `Profile` object or a dictionary containing the
        actual Batsim profile configuration).

        :param workload_name: The name of the workload which should be chosen if the profiles should be cached, since profiles are always related to their workload. If omitted a dynamically generated name for the workload will be used.
        """
        return DynamicJob(*args, **kwargs)

    def create_sub_job(self, *args, **kwargs):
        """Create a dynamic job as a sub job.

        A sub job has no other meaning besides annotating that this new dynamic job is somehow related
        to its parent job. A use case could be to generate various dynamic jobs to prepare the running of a jub
        and create these dynamic jobs as sub jobs to make it easier keeping track of the related dynamic jobs.

        :param requested_resources: the number of requested resources.

        :param requested_time: the number of requested time (walltime)

        :param profile: The profile object (either a `Profile` object or a dictionary containing the
        actual Batsim profile configuration).

        :param workload_name: The name of the workload which should be chosen if the profiles should be cached, since profiles are always related to their workload. If omitted a dynamically generated name for the workload will be used.
        """
        return DynamicJob(*args, parent_job=self, **kwargs)


class DynamicJob(Job):
    """A DynamicJob may be used to construct dynamic jobs afterwards submitted to the scheduler.
    It has no related batsim_job since it is not known by Batsim yet. Instead it should be submitted
    and will be bounced back to the scheduler as a job known by Batsim and can be executed in this state.

    :param requested_resources: the number of requested resources.

    :param requested_time: the number of requested time (walltime)

    :param profile: The profile object (either a `Profile` object or a dictionary containing the
    actual Batsim profile configuration).

    :param workload_name: The name of the workload which should be chosen if the profiles should be cached, since profiles are always related to their workload. If omitted a dynamically generated name for the workload will be used.

    :param parent_job: the parental job object if this job should be a sub job
    """

    def __init__(
            self,
            requested_resources,
            requested_time,
            profile,
            workload_name=None,
            parent_job=None):
        super().__init__(parent_job=parent_job)
        self._job_id = None
        self._requested_resources = requested_resources
        self._requested_time = requested_time
        self._profile = profile
        self._workload_name = workload_name

        self._submitted = False

        if parent_job is not None:
            parent_job._jobs_list.update_element(self)

    @property
    def id(self):
        return self._job_id

    @property
    def submit_time(self):
        return None

    @property
    def requested_time(self):
        return self._requested_time

    @property
    def requested_resources(self):
        return self._requested_resources

    @property
    def profile(self):
        return self._profile

    @property
    def workload_name(self):
        return self._workload_name

    @property
    def finish_time(self):
        return None

    @property
    def state(self):
        return BatsimJob.State.UNKNOWN

    @property
    def kill_reason(self):
        return None

    @property
    def return_code(self):
        return None

    @property
    def completed(self):
        return False

    @property
    def submitted(self):
        return self._submitted

    @property
    def is_dynamic_submission_request(self):
        return True

    @property
    def is_dynamic_job(self):
        return True

    def submit(self, scheduler):
        """Marks a dynamic job for submission in the `scheduler`."""
        assert not self._submitted, "Dynamic job was already submitted"
        scheduler.jobs.add(self)
        self._jobs_list = scheduler.jobs
        self._scheduler = scheduler

        # The profile object will be executed if it is no dictionary already to
        # allow complex Profile objects.
        profile = self._profile
        if not isinstance(profile, dict):
            profile = profile(self._scheduler)

        parent_job_id = None
        try:
            parent_job_id = self.parent_job.id
        except AttributeError:
            pass

        self._scheduler.info(
            "Submit dynamic job ({job})",
            job=self,
            subjob_of=parent_job_id,
            subjob_of_obj=self.parent_job,
            is_subjob=(parent_job_id is not None),
            type="dynamic_job_submit")
        self._job_id = self._scheduler._batsim.submit_job(
            self._requested_resources,
            self._requested_time,
            profile,
            self._profile.name,
            self._workload_name)

        if self.parent_job:
            self.parent_job._sub_jobs.append(self)
            self.parent_job._jobs_list.update_element(self)

        self._submitted = True
        self._jobs_list.update_element(self)


class Jobs(ObserveList):
    """Helper class implementing parts of the python list API to manage the jobs.

       :param from_list: a list of `Job` objects to be managed by this wrapper.
    """

    def __init__(self, *args, **kwargs):
        self._job_map = {}
        super().__init__(*args, **kwargs)

    @property
    def runnable(self):
        """Returns all jobs which are runnable."""
        return self.filter(runnable=True)

    @property
    def running(self):
        """Returns all jobs which are currently running."""
        return self.filter(running=True)

    @property
    def open(self):
        """Returns all jobs which are currently open."""
        return self.filter(open=True)

    @property
    def completed(self):
        """Returns all jobs which are completed."""
        return self.filter(completed=True)

    @property
    def rejected(self):
        """Returns all jobs which were rejected."""
        return self.filter(rejected=True)

    @property
    def scheduled(self):
        """Returns all jobs which were scheduled."""
        return self.filter(scheduled=True)

    @property
    def killed(self):
        """Returns all jobs which were killed."""
        return self.filter(killed=True)

    @property
    def submitted(self):
        """Returns all jobs which are submitted to Batsim."""
        return self.filter(submitted=True)

    @property
    def dynamic_submission_request(self):
        """Returns all jobs which are requests for dynamic submissions."""
        return self.filter(dynamic_submission_request=True)

    def __getitem__(self, items):
        if isinstance(items, slice):
            return self.create(self._data[items])
        else:
            return self._job_map[items]

    def __delitem__(self, index):
        job = self._job_map[items]
        self.remove(job)

    def __setitem__(self, index, element):
        raise ValueError("Cannot override a job id")

    def _element_new(self, job):
        if job.id:
            self._job_map[job.id] = job

    def _element_del(self, job):
        if job.id:
            del self._job_map[job.id]

    def filter(
            self,
            *args,
            runnable=False,
            running=False,
            open=False,
            completed=False,
            rejected=False,
            scheduled=False,
            killed=False,
            submitted=False,
            dynamic_submission_request=False,
            **kwargs):
        """Filter the jobs lists to search for jobs.

        :param runnable: whether the job is runnable (open and dependencies fulfilled).

        :param running: whether the job is currently running.

        :param open: whether the job is still open.

        :param completed: whether the job has already been completed.

        :param rejected: whether the job has already been rejected.

        :param scheduled: whether the job has already been scheduled.

        :param killed: whether the job has been sent for killing.

        :param submitted: whether the job has been submitted.

        :param dynamic_submission_request: whether the job is a request for dynamic submission.
        """

        # Yield all jobs if not filtered
        if True not in [
                runnable, running,
                open, completed,
                rejected,
                scheduled,
                killed,
                submitted,
                dynamic_submission_request]:
            runnable = True
            running = True
            open = True
            completed = True
            rejected = True
            scheduled = True
            killed = True
            submitted = True
            dynamic_submission_request = True

        # Filter jobs
        def filter_jobs(jobs):
            for j in jobs:
                if j.is_dynamic_submission_request:
                    if dynamic_submission_request or submitted:
                        yield j
                elif j.running:
                    if running or submitted:
                        yield j
                elif j.completed:
                    if completed or submitted:
                        yield j
                elif j.rejected:
                    if rejected or submitted:
                        yield j
                elif j.scheduled:
                    if scheduled or submitted:
                        yield j
                elif j.killed:
                    if killed or submitted:
                        yield j
                elif j.runnable:
                    if runnable or open or submitted:
                        yield j
                elif j.open:
                    if open or submitted:
                        yield j
                elif j.submitted:
                    if submitted:
                        yield j

        return self.create(
            filter_list(
                self._data,
                [filter_jobs],
                *args,
                **kwargs))
