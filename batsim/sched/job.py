"""
    batsim.sched.job
    ~~~~~~~~~~~~~~~~

    This module provides general abstractions to manage jobs (either created by Batsim
    or by the user to submit dynamic jobs).

"""
from batsim.batsim import Job as BatsimJob, Batsim

from .utils import ObserveList, filter_list
from .alloc import Allocation


class Job:
    """A job is a wrapper around a batsim job to extend the basic API of Pybatsim with more
    object oriented approaches on the implementation of the scheduler.

    :param batsim_job: the batsim job object from the underlying Pybatsim scheduler.
    """

    def __init__(self, batsim_job=None, parent_job=None):
        self._batsim_job = batsim_job
        self._parent_job = parent_job

        self._marked_for_scheduling = False
        self._scheduled = False

        self._killed = False
        self._marked_for_killing = False

        self._rejected = False
        self._marked_for_rejection = False
        self._rejected_reason = None

        self._sub_jobs = []

        self._own_dependencies = []

        self._allocation = None

    def free_all(self):
        """Free all reserved resources."""
        assert self._allocation is not None

        for res in self.allocation.resources:
            self.free(res)

    def free(self, resource, recursive_call=False):
        """Free the given `resource` object or fails if the resource is currently
        not reserved by this job.
        """
        assert self._batsim_job
        assert self._allocation is not None

        # To free resources the job does either have to be not submitted yet
        # or the job has to be completed (i.e. the job status is set by
        # batsim).
        assert (
            not self.marked_for_scheduling and not self.scheduled) or self.completed

        if not recursive_call:
            resource.free(self, recursive_call=True)

        if len(self.allocation) == 0:
            self._allocation = None

    def reserve(self, resource, recursive_call=False):
        """Reserves a given `resource` to ensure exclusive access."""
        assert self._batsim_job
        assert self._allocation is None
        assert self.open

        self._allocation = Allocation(self)

        if not recursive_call:
            resource.allocate(self, recursive_call=True)

    def dependencies_fulfilled(self, jobs):
        for dep in self.get_resolved_dependencies(jobs):
            if not isinstance(dep, Job) or not dep.completed:
                return False
        return True

    def get_resolved_dependencies(self, jobs):
        result = []
        for dep in self.dependencies:
            jobparts = self.id.split("!")
            workload_name = "!".join(jobparts[:len(jobparts) - 1])
            job_id = jobparts[-1]

            dep_job_id = str(workload_name) + "!" + str(dep)
            try:
                dep_job = jobs[dep_job_id]
                result.append(dep_job)
            except KeyError:
                result.append(dep_job_id)
        return tuple(result)

    @property
    def dependencies(self):
        return tuple(
            (self.get_job_data("deps") or []) +
            self._own_dependencies)

    def add_dependency(self, job):
        assert self.open
        return self._own_dependencies.append(job)

    def remove_dependency(self, job):
        assert self.open
        return self._own_dependencies.remove(job)

    @property
    def qos(self):
        return self.get_job_data("qos") or ""

    @property
    def user(self):
        return self.get_job_data("user") or ""

    @property
    def partition(self):
        return self.get_job_data("partition") or ""

    @property
    def job_group(self):
        return self.get_job_data("group") or ""

    @property
    def comment(self):
        return self.get_job_data("comment") or ""

    @property
    def application(self):
        return self.get_job_data("application") or ""

    def get_job_data(self, key):
        if not self._batsim_job:
            return None
        return self._batsim_job.json_dict.get(key, None)

    def is_runnable(self, jobs):
        """Whether the job is open and has only fulfilled dependencies."""
        return self.dependencies_fulfilled(jobs) and self.open

    @property
    def open(self):
        """Whether or not this job is still open."""
        return True not in [
            self.completed,
            self.marked_for_scheduling, self.scheduled,
            self.marked_for_killing, self.killed,
            self.marked_for_rejection, self.rejected]

    @property
    def parent_job(self):
        return self._parent_job

    @property
    def sub_jobs(self):
        return tuple(self._sub_jobs)

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
            if self._batsim_job.requested_resources < len(self.allocation):
                scheduler.warn(
                    "Scheduling of job ({job}) is postponed since not enough resources are allocated",
                    job=self,
                    type="job_starting_postponed_too_few_resources")
                return

            if not scheduler.has_time_sharing:
                for r in self.allocation.resources:
                    if len(r.allocations) != 1:
                        raise ValueError(
                            "Time sharing is not enabled in Batsim")

            self.allocation.allocate()

            alloc = []
            for res in self.allocation[:self._batsim_job.requested_resources]:
                alloc.append(res.id)

            scheduler._batsim.start_jobs(
                [self._batsim_job], {self._batsim_job.id: alloc})

            scheduler.info(
                "Scheduled job ({job})",
                job=self,
                type="job_scheduled")
            self._marked_for_scheduling = False
            self._scheduled = True

    def _do_complete_job(self, scheduler):
        scheduler.info("Remove completed job and free resources: {job}",
                       job=self, type="job_completed")
        self.allocation.free()

    def move_properties_from(self, otherjob):
        parent_job = otherjob.parent_job
        if parent_job:
            parent_job._sub_jobs.append(self)
            self._parent_job = parent_job
            parent_job._sub_jobs.remove(otherjob)
        self._own_dependencies = list(otherjob._own_dependencies)

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
        assert self.open

        self._marked_for_rejection = True
        self._rejected_reason = reason

    def _do_reject(self, scheduler):
        """Internal method to execute the rejecting of the job."""
        if self.marked_for_rejection and not self.rejected:
            scheduler.info(
                "Rejecting job ({job}), reason={reason}",
                job=self, reason=self.rejected_reason, type="job_rejection")
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
        assert self.running
        self._killed = True

    @property
    def running(self):
        return not self.open and self.scheduled and not self.completed

    def _do_kill(self, scheduler):
        """Internal method to execute the killing of the job."""
        if self.marked_for_killing and not self.killed:
            scheduler.info("Killing job ({job})", job=self, type="job_killing")
            scheduler._batsim.kill_jobs([self._batsim_job])

            self._killed = True
            self._marked_for_killing = True

    @property
    def allocation(self):
        """Returns the current allocation of this job."""
        return self._allocation

    def schedule(self, resource=None):
        """Mark this job for scheduling. This can also be done even when not enough resources are
        reserved. The job will not be sent to Batsim until enough resources were reserved."""
        assert self._batsim_job
        assert self.open

        if resource:
            self.reserve(resource)

        self._marked_for_scheduling = True

    def change_state(self, scheduler, state, kill_reason=""):
        """Change the state of a job. This is only needed in rare cases where the real job
        should not be executed but instead the state should be set manually.
        """
        assert self._batsim_job
        assert self.open
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

    def create_sub_job(self, *args, **kwargs):
        return DynamicJob(*args, parent_job=self, **kwargs)

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
            workload_name=None,
            parent_job=None):
        super().__init__(parent_job=parent_job)
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

            parent_job_id = None
            try:
                parent_job_id = self.parent_job.id
            except AttributeError:
                pass

            scheduler.info(
                "Submit dynamic job ({job})",
                job=self,
                subjob_of=parent_job_id,
                subjob_of_obj=self.parent_job,
                is_subjob=(parent_job_id is not None),
                type="dynamic_job_submit")
            self._user_job_id = scheduler._batsim.submit_job(
                self._user_requested_resources,
                self._user_requested_time,
                profile,
                self._user_profile_name,
                self._user_workload_name)

            if self.parent_job:
                self.parent_job._sub_jobs.append(self)

            self._dyn_submitted = True
            self._dyn_marked_submission = False


class Jobs(ObserveList):
    """Helper class implementing parts of the python list API to manage the jobs.

       :param from_list: a list of `Job` objects to be managed by this wrapper.
    """

    def __init__(self, *args, **kwargs):
        self._job_map = {}
        super().__init__(*args, **kwargs)

    def __getitem__(self, items):
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

    @property
    def runnable(self):
        return self.filter(runnable=True)

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
            runnable=False,
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

        :param runnable: whether the job is runnable (open and dependencies fulfilled).

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
                runnable,
                open, completed,
                rejected, marked_for_rejection,
                scheduled, marked_for_scheduling,
                killed, marked_for_killing,
                dynamically_submitted, marked_for_dynamic_submission]:
            runnable = True

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
                    elif j.is_runnable(self):
                        if runnable:
                            yield j

        return self.create(
            filter_list(
                self._data,
                [filter_jobs],
                cond,
                limit,
                min,
                num))
