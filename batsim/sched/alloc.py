"""
    batsim.sched.allocs
    ~~~~~~~~~~~~~~~~~~~

    This module generalises the allocation of resources.
"""
import sys

from .resource import Resources, Resource
from .utils import ListView


class Allocation:
    """An allocation is a reservation of resources. To finally allocate a set of
    resources also a job has to be assigned. It is valid for a given walltime.

    :param start_time: the start time of the allocation
    :param walltime: the duration of the allocation
    :param resources: the initial set of resources to assign to the allocation
    :param job: the initial job assignment for the allocation
    """

    def __init__(self, start_time, walltime=None, resources=[], job=None):
        self._job = None
        self._resources = []

        self._scheduler = None

        self._allocated = False
        self._previously_allocated = False

        self._allocated_resources = set()

        assert start_time > 0, "Invalid start time for the allocation was given"
        self._start_time = start_time
        self._end_time = None
        self._walltime = walltime

        if isinstance(resources, Resource):
            self.add_resource(resources)
        else:  # Assume some kind of list type
            for r in resources:
                self.add_resource(r)

        if job is not None:
            if walltime is None:
                self._walltime = job.requested_time
            self._reserve_job_on_allocation(job)

    @property
    def job(self):
        """The assigned job."""
        return self._job

    @property
    def resources(self):
        """The list of assigned resources."""
        return ListView(self._resources)

    @property
    def allocated(self):
        """Whether or not this allocation is currently allocated (active)."""
        return self._allocated

    @property
    def start_time(self):
        """The start time of this allocation."""
        return self._start_time

    @start_time.setter
    def start_time(self, new_time):
        assert not self.allocated and not self.previously_allocated, "Allocation is in invalid state"
        assert self._end_time is None or new_time < self.end_time, "An allocation can not start after its end time"
        self._start_time = new_time

    @property
    def end_time(self):
        """The end time of this allocation (either explicit or based on the walltime)."""
        return self._end_time or (
            self.start_time + (self.walltime or sys.maxsize))

    @property
    def walltime(self):
        """The duration of this allocation."""
        return self._walltime

    @walltime.setter
    def walltime(self, new_time):
        assert not self.allocated and not self.previously_allocated, "Allocation is in invalid state"
        assert new_time > 0, "The walltime has to be > 0"
        self._walltime = new_time

    @property
    def allocated_resources(self):
        """The list of allocated resources (currently assigned as computing)."""
        return ListView(self._allocated_resources)

    @property
    def previously_allocated(self):
        """Whether or not this allocation was previously allocated (active) but was
        already freed.
        """
        return self._previously_allocated

    def __len__(self):
        """The number of resources in this allocation."""
        return len(self._resources)

    def __getitem__(self, items):
        """Return parts of the resources."""
        return self._resources[items]

    def starts_in_future(self, scheduler):
        """Whether or not this allocation will start in the future."""
        return self.start_time > scheduler.time

    def started_in_past(self, scheduler):
        """Whether or not this allocation has a start date in the past (which does not
        mean that it was active).
        """
        return self.start_time < scheduler.time

    def starts_now(self, scheduler):
        """Whether or not this allocation is set for starting at the current scheduling time
        (this does not mean that it is scheduled).
        """
        return self.start_time == scheduler.time

    def _reserve_job_on_allocation(self, newjob):
        """Reserves the given job to run on this allocation. This does not automatically schedule the job.

        :param newjob: the job to be allocated on this allocation
        """
        assert not self.allocated and not self.previously_allocated, "Allocation is in invalid state"
        assert self._job is None, "Job is not assigned"
        assert newjob.open, "Job is not in state open"
        assert self.fits_job(newjob), "Job does not fit in allocation"

        self._job = newjob

    def fits_job(self, job):
        """Determines whether or not the job fits in the remaining walltime of this allocation.

        :param job: the job for which the walltime should be evaluated.
        """
        return self.fits_job_for_remaining_time(job, self.walltime)

    def fits_job_for_remaining_time(self, job, remaining_time=None):
        """Whether or not the job fits in the remaining time frame of this allocation.

        :param job: the job to be checked whether or not it fits

        :param remaining_time: the time which still remains on this allocation. If not given the
        actual remaining time is returned.
        """
        if remaining_time is None:
            elapsed_time = job._scheduler.time - self.start_time
            if elapsed_time < 0:
                remaining_time = self.walltime
            else:
                remaining_time = self.walltime - elapsed_time

        return job.fits_in_frame(remaining_time, len(self))

    def _free_job_from_allocation(self):
        """Frees a job from the current allocation. This only works if the allocation
        was never active and if the job is set.
        """
        assert not self.allocated and not self.previously_allocated, "Allocation is in invalid state"
        assert self._job is not None, "No job set in allocation"

        self._job = None

    def overlaps_with(self, other_allocation):
        """Whether or not this allocation overlaps with the time frame of another allocation.

        :param other_allocation: the allocation to be checked for overlaps
        """
        s1 = self.start_time
        e1 = self.end_time

        s2 = other_allocation.start_time
        e2 = other_allocation.end_time

        if s1 < s2:
            if e1 >= s2:
                # other_allocation is part of the current allocation
                return True
        else:
            if e2 >= s1:
                # current allocation is part of the other allocation
                return True
        return False

    def add_resource(self, resource):
        """Adds a resource to this allocation.

        :param resource: the resource to be added
        """
        assert not self.allocated and not self.previously_allocated, "Allocation is in invalid state"
        resource._do_add_allocation(self)
        self._resources.append(resource)

    def remove_resource(self, resource):
        """Removes a resource from this allocation.

        :param resource: the resource to be removed
        """
        assert not self.allocated and not self.previously_allocated, "Allocation is in invalid state"
        resource._do_remove_allocation(self)
        self._resources.remove(resource)

    def remove_all_resources(self):
        """Removes all resources from this allocation."""
        assert not self.allocated and not self.previously_allocated, "Allocation is in invalid state"
        for r in self._resources.copy():
            self.remove_resource(r)

    def allocate(self, scheduler, range1, *more_ranges):
        """Mark node ranges from this allocation as `computing` and set the allocation
        to `allocated`.

        Since there can be more nodes in an allocation object than the actually used hosts this
        can method is used to tell the scheduler which parts of the allocation are in use.

        :param scheduler: the scheduler to which the allocation is related to

        :param range1, more_ranges: the ranges of nodes which should be allocated
        """
        assert not self._previously_allocated and not self._allocated, "Allocation is in invalid state"
        assert self._job is not None, "No job set in allocation"

        time = scheduler.time
        assert self.start_time <= time and self.end_time > time, "The allocation lies not in the current simulation time frame"

        for r in ([range1] + list(more_ranges)):
            for i in r:
                res = self._resources[i]
                if res in self._allocated_resources:
                    self._allocated_resources.clear()
                    scheduler.fatal(
                        "Resource ranges in allocation are invalid: {res}",
                        res=res,
                        type="resource_ranges_invalid")
                self._allocated_resources.add(res)
                res._do_allocate_allocation(self)
        self._allocated = True
        self._scheduler = scheduler

    def free(self):
        """Either free a finished allocation or clear an allocation which was not started yet.

        :param scheduler: The scheduler argument is required when a finished allocation
        should be freed. It is not required to clear an allocation which was not started
        yet.
        """
        assert not self._previously_allocated, "Allocation is in invalid state"
        if not self._allocated:
            if self._job is not None:
                self._free_job_from_allocation()
            self.remove_all_resources()
            return

        for r in self.resources.copy():
            r._do_free_allocation(self)

        self._end_time = self._scheduler.time
        self._allocated = False
        self._previously_allocated = True

    def __str__(self):
        jobid = None
        if self.job:
            jobid = self.job.id

        resources = []
        for r in self._resources:
            resources.append(r.name)

        allocated = []
        for r in self._allocated_resources:
            allocated.append(r.name)

        return (
            "<Allocation starttime:{} endtime:{} walltime:{} resources:{} allocated:{} job:{}>"
            .format(
                self.start_time, self.end_time, self.walltime, resources, allocated, jobid))