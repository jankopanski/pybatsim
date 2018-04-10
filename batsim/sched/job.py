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
from .profiles import Profiles, Profile
from .workloads import WorkloadDescription


class Job:
    """A job is a wrapper around a batsim job to extend the basic API of Pybatsim with more
    object oriented approaches on the implementation of the scheduler.

    :param number: the number of this job in the queue.

    :param batsim_job: the batsim job object from the underlying Pybatsim scheduler.

    :param scheduler: the associated scheduler managing this job.

    :param job_list: the main job list where this job is contained

    :param parent_job: the parent job if this is a sub job
    """

    State = BatsimJob.State

    def __init__(
            self,
            number=None,
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

        self._own_dependencies = []

        self._allocation = None
        self._start_time = None

        self._messages = MessageBuffer()

        self._profile = None

        self._comment = None

        self._number = number

        self._workload_description = None
        self._subjobs_workload = None

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
        except AttributeError:
            return default
        except KeyError:
            try:
                return self._batsim_job.__dict__[key]
            except KeyError:
                return default

    @property
    def number(self):
        """The number of this job (place in the queue)."""
        return self._number

    @property
    def messages(self):
        """The buffer of incoming messages"""
        return self._messages

    @property
    def sub_jobs_workload(self):
        """The workload containing sub jobs."""
        if self._subjobs_workload is None:
            self._subjobs_workload = SubjobWorkload(self, name=self.id.replace(
                Batsim.WORKLOAD_JOB_SEPARATOR, Batsim.WORKLOAD_JOB_SEPARATOR_REPLACEMENT))
        return self._subjobs_workload

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
    def comment(self):
        """The comment will be written to the log file on job completion.
        This field can be used to write additional data to the out_sched_jobs file."""
        return self._comment or self.get_job_data("comment")

    @comment.setter
    def comment(self, value):
        self._comment = value

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
            self.rejected]

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
        return Jobs(
            [j.job for j in self.sub_jobs_workload if j.job is not None])

    @property
    def running(self):
        """Whether or not this job is currently running."""
        return self.scheduled and not self.completed and not self.killed

    @property
    def completed(self):
        """Whether or not this job has been completed."""
        return self.state in [
            BatsimJob.State.COMPLETED_KILLED,
            BatsimJob.State.COMPLETED_WALLTIME_REACHED,
            BatsimJob.State.COMPLETED_SUCCESSFULLY,
            BatsimJob.State.COMPLETED_FAILED]

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
        return self.state == BatsimJob.State.RUNNING or (
            not self.open and self.scheduled and not self.completed)

    @property
    def success(self):
        """Whether this job has successfully finished its execution."""
        return self.state == BatsimJob.State.COMPLETED_SUCCESSFULLY

    @property
    def failure(self):
        """Whether this job has failed its execution."""
        return self.state in [
            BatsimJob.State.COMPLETED_KILLED,
            BatsimJob.State.COMPLETED_WALLTIME_REACHED,
            BatsimJob.State.COMPLETED_FAILED]

    @property
    def allocation(self):
        """Returns the current allocation of this job."""
        return self._allocation

    @property
    def id(self):
        """The id of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.id

    @id.setter
    def id(self, value):
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        self._batsim_job.id = value

    @property
    def submit_time(self):
        """The time of submission of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.submit_time

    @submit_time.setter
    def submit_time(self, value):
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        self._batsim_job.submit_time = value

    @property
    def requested_time(self):
        """The requested time of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.requested_time

    @requested_time.setter
    def requested_time(self, value):
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        self._batsim_job.requested_time = value

    @property
    def requested_resources(self):
        """The requested resources of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.requested_resources

    @requested_resources.setter
    def requested_resources(self, value):
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        self._batsim_job.requested_resources = value

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

    @finish_time.setter
    def finish_time(self, value):
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        self._batsim_job.finish_time = value

    @property
    def state(self):
        """The state of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        if self._changed_state is not None:
            return self._changed_state
        return self._batsim_job.job_state

    @property
    def return_code(self):
        """The return code of this job as known by Batsim."""
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        return self._batsim_job.return_code

    @return_code.setter
    def return_code(self, value):
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        self._batsim_job.return_code = value

    @property
    def is_dynamic_job(self):
        """Whether or not this job is a dynamic job."""
        return self._workload_description is not None

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

    @property
    def progress(self):
        return self._batsim_job.progress

    @progress.setter
    def progress(self, value):
        self._batsim_job.progress = value

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

        :param resource: either a single `Resource` a list in the form of a
                         `Resources` object or an `Allocation`
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
                                self._scheduler.fatal(
                                    "Time sharing is not enabled in Batsim (resource allocations are overlapping: own={own}, other={other})",
                                    own=a1, other=a2,
                                    type="schedule_resource_allocations_overlapping")

        if not self.allocation.fits_job_for_remaining_time(self):
            self._scheduler.fatal(
                "Job does not fit in the remaining time frame of the allocation ({job})",
                job=self, type="job_does_not_fit")

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
            if res.num_active != 1:
                self._scheduler.fatal(
                    "Scheduled resource {res} was already part of a Batsim allocation",
                    res=res, type="resource_already_allocated")
            alloc.append(res.id)

        self._scheduler.debug(
            "Start job {batsim_job} on {resources}",
            batsim_job=self._batsim_job,
            resources=alloc,
            type="start_job")

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

    def change_state(self, state, return_code=None):
        """Change the state of a job. This is only needed in rare cases where the real job
        should not be executed but instead the state should be set manually.
        """
        assert self._batsim_job, "Batsim job is not set => job was not correctly initialised"
        self._scheduler._batsim.change_job_state(
            self._batsim_job, state)
        self._changed_state = state

        if state == Job.State.RUNNING:
            self._start_time = self._scheduler.time
            self._scheduled = True
        elif state in [Job.State.COMPLETED_FAILED,
                       Job.State.COMPLETED_SUCCESSFULLY,
                       Job.State.COMPLETED_WALLTIME_REACHED,
                       Job.State.COMPLETED_KILLED]:
            self._batsim_job.finish_time = self._scheduler.time
            self._batsim_job.return_code = (
                return_code or 0 if state == Job.State.COMPLETED_SUCCESSFULLY else 1)
        self._jobs_list.update_element(self)

    def __str__(self):
        data = self.to_json_dict()

        return (
            "<Job {}; number:{} queue:{} sub:{} reqtime:{} res:{} prof:{} start:{} fin:{} stat:{} ret:{} comment:{}>"
            .format(
                data["id"], data["number"], data["queue_number"],
                data["submit_time"], data["requested_time"],
                data["requested_resources"], data["profile"],
                data["start_time"],
                data["finish_time"], data["state"],
                data["return_code"],
                data["comment"]))

    def to_json_dict(self, recursive=True):
        """Returns a dict representation of this object.

        :param recursive: whether object references should be resolved
        """
        profile = None
        profile_name = None
        if self.profile is not None:
            profile = self.profile.to_dict()
            profile_name = self.profile.name

        state = None
        if self.state is not None:
            state = self.state.name

        split_id = self.id.split(Batsim.WORKLOAD_JOB_SEPARATOR)

        parent_id = ""
        parent_workload_name = ""
        parent_number = ""

        if self.parent_job:
            parent_id = self.parent_job.id
            parent_split_id = parent_id.split(Batsim.WORKLOAD_JOB_SEPARATOR)
            parent_workload_name = parent_split_id[0]
            parent_number = parent_split_id[1]

        return {
            "id": self.id,
            "workload_name": split_id[0],
            "number": split_id[1],
            "parent_id": parent_id,
            "parent_workload_name": parent_workload_name,
            "parent_number": parent_number,
            "queue_number": self.number,
            "submit_time": self.submit_time,
            "requested_time": self.requested_time,
            "requested_resources": self.requested_resources,
            "profile": profile,
            "profile_name": profile_name,
            "start_time": self.start_time,
            "finish_time": self.finish_time,
            "state": state,
            "success": True if self.success else False,
            "return_code": self.return_code,
            "comment": self.comment
        }

    def submit_sub_job(self, *args, **kwargs):
        job = self.sub_jobs_workload.new_job(*args, **kwargs)
        self.sub_jobs_workload.prepare()
        job.submit(self._scheduler)


class SubjobWorkload(WorkloadDescription):

    def __init__(self, parent_job, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parent_job = parent_job

    def update_job(self, jobdesc, job):
        job._parent_job = self._parent_job


class Jobs(ObserveList):
    """Helper class implementing parts of the python list API to manage the jobs.

       :param from_list: a list of `Job` objects to be managed by this wrapper.
    """

    def __init__(self, *args, **kwargs):
        self._job_map = {}
        super().__init__(*args, **kwargs)

    def __eq__(self, other):
        if type(other) is not Jobs:
            return False
        if len(self._job_map) != len(other._job_map):
            return False
        return all(
            [me.id == him.id
             for me, him
             in zip(self._job_map.values(), other._job_map.values())])

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
    def dynamic_job(self):
        """Returns all jobs which are dynamic jobs."""
        return self.filter(dynamic_job=True)

    @property
    def static_job(self):
        """Returns all jobs which are static jobs."""
        return self.filter(static_job=True)

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
            dynamic_job=False,
            static_job=False,
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

        :param dynamic_job: whether the job is a dynamic job.

        :param static_job: whether the job is a static job.
        """

        # Yield all jobs if not filtered
        if True not in [
                runnable, running,
                open, completed,
                rejected,
                scheduled,
                killed,
                submitted,
                dynamic_job,
                static_job]:
            runnable = True
            running = True
            open = True
            completed = True
            rejected = True
            scheduled = True
            killed = True
            submitted = True
            dynamic_job = True
            static_job = True

        # Filter jobs
        def filter_jobs(jobs, **kwargs):
            for j in jobs:
                if dynamic_job:
                    if j.is_dynamic_job:
                        yield j
                        continue
                if static_job:
                    if not j.is_dynamic_job:
                        yield j
                        continue
                if running:
                    if j.running:
                        yield j
                        continue
                if completed:
                    if j.completed:
                        yield j
                        continue
                if rejected:
                    if j.rejected:
                        yield j
                        continue
                if scheduled:
                    if j.scheduled:
                        yield j
                        continue
                if killed:
                    if j.killed:
                        yield j
                        continue
                if runnable:
                    if j.runnable:
                        yield j
                        continue
                if open:
                    if j.open:
                        yield j
                        continue
                if submitted:
                    if j.submitted:
                        yield j
                        continue

        return self.create(
            filter_list(
                self._data,
                [filter_jobs],
                *args,
                **kwargs))
