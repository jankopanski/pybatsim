"""
    batsim.sched.allocs
    ~~~~~~~~~~~~~~~~~~~

    This module generalises the allocation of resources.
"""
import sys

from .resource import Resources, Resource


class Allocation:

    def __init__(self, start_time, walltime=None, resources=[], job=None):
        self._job = None
        self._resources = []

        self._allocated = False
        self._previously_allocated = False

        self._allocated_resources = set()

        assert start_time > 0
        self._start_time = start_time
        self._end_time = None
        self._walltime = walltime

        if isinstance(resources, (list, Resources)):
            for r in resources:
                self.add_resource(r)
        elif isinstance(resources, Resource):
            self.add_resource(resources)

        if job is not None:
            if walltime is None:
                self._walltime = job.requested_time
            self._reserve_job_on_allocation(job)

    @property
    def job(self):
        return self._job

    def starts_in_future(self, scheduler):
        return self.start_time > scheduler.time

    def started_in_past(self, scheduler):
        return self.start_time < scheduler.time

    def starts_now(self, scheduler):
        return self.start_time == scheduler.time

    def _reserve_job_on_allocation(self, newjob):
        assert not self.allocated and not self.previously_allocated
        assert self._job is None
        assert newjob.open
        assert self.fits_job(newjob), "Job does not fit in allocation"

        self._job = newjob

    def _free_job_from_allocation(self):
        assert not self.allocated and not self.previously_allocated
        assert self._job is not None

        self._job = None

    @property
    def resources(self):
        return tuple(self._resources)

    @property
    def allocated(self):
        return self._allocated

    def fits_job(self, job):
        return self.fits_job_for_remaining_time(job, self.walltime)

    def fits_job_for_remaining_time(self, job, remaining_time=None):
        if remaining_time is None:
            elapsed_time = job._scheduler.time - self.start_time
            if elapsed_time < 0:
                remaining_time = self.walltime
            else:
                remaining_time = self.walltime - elapsed_time

        return remaining_time >= job.requested_time and len(
            self) >= job.requested_resources

    def overlaps_with(self, other_allocation):
        s1 = self.start_time
        e1 = self.end_time

        s2 = other_allocation.start_time
        e2 = other_allocation.end_time

        if s1 < s2:
            if e1 >= s2:
                return True
        else:
            if e2 >= s1:
                return True
        return False

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, new_time):
        assert not self.allocated and not self.previously_allocated
        assert self._end_time is None or new_time < self.end_time
        self._start_time = new_time

    @property
    def end_time(self):
        return self._end_time or (
            self.start_time + (self.walltime or sys.maxsize))

    @property
    def walltime(self):
        return self._walltime

    @walltime.setter
    def walltime(self, new_time):
        assert not self.allocated and not self.previously_allocated
        assert new_time > 0
        self._walltime = new_time

    @property
    def allocated_resources(self):
        return tuple(self._allocated_resources)

    @property
    def previously_allocated(self):
        return self._previously_allocated

    def add_resource(self, resource):
        assert not self.allocated and not self.previously_allocated
        resource._do_add_allocation(self)
        self._resources.append(resource)

    def remove_resource(self, resource):
        assert not self.allocated and not self.previously_allocated
        resource._do_remove_allocation(self)
        self._resources.remove(resource)

    def remove_all_resources(self):
        assert not self.allocated and not self.previously_allocated
        for r in self._resources[:]:
            self.remove_resource(r)

    def __len__(self):
        return len(self._resources)

    def __getitem__(self, items):
        return self._resources[items]

    def allocate(self, scheduler, range1, *more_ranges):
        assert not self._previously_allocated and not self._allocated
        assert self._job is not None

        time = scheduler.time
        assert self.start_time <= time and self.end_time > time

        for r in ([range1] + list(more_ranges)):
            for i in r:
                res = self._resources[i]
                if res in self._allocated_resources:
                    self._allocated_resources.clear()
                    raise ValueError(
                        "Resource ranges in allocation are invalid")
                self._allocated_resources.add(res)
                res._do_allocate_allocation(self)
        self._allocated = True

    def free(self, scheduler=None):
        if not self._allocated and not self._previously_allocated:
            self.remove_all_resources()
            if self._job is not None:
                self._free_job_from_allocation()
            return
        assert scheduler is not None

        for r in self.resources:
            r._do_free_allocation(self)

        self._end_time = scheduler.time
        self._allocated = False
        self._previously_allocated = True
