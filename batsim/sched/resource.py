"""
    batsim.sched.resource
    ~~~~~~~~~~~~~~~~~~~~~

    This module provides an abstraction around resources to keep track of allocations.

"""
from enum import Enum

from .utils import ObserveList, filter_list


class Resource:
    """A resource is an introduced abstraction to easier keep track of resource states and
    job allocations.

    :param scheduler: the associated scheduler managing this resource.

    :param batsim_id: the id of this resource.
    """

    class State(Enum):
        SLEEPING = 0
        IDLE = 1
        COMPUTING = 2
        TRANSITING_FROM_SLEEPING_TO_COMPUTING = 3
        TRANSITING_FROM_COMPUTING_TO_SLEEPING = 4

    def __init__(self, scheduler, id, name, state, properties):
        self._scheduler = scheduler
        self._id = id
        self._name = name
        try:
            self._state = Resource.State[(state or "").upper()]
        except KeyError:
            raise ValueError("Invalid machine state: {}, {}={}"
                             .format(id, name, state))
        self._properties = properties

        self._allocations = set()

        self._pstate = None
        self._old_pstate = None
        self._pstate_update_in_progress = False
        self._pstate_update_request_necessary = False

    @property
    def id(self):
        return self._id

    @property
    def is_allocated(self):
        return bool(self._allocations)

    @property
    def allocations(self):
        return tuple(self._allocations)

    @property
    def computing(self):
        for alloc in self._allocations:
            if self in alloc.allocated_resources:
                return True
        return False

    def first_frame_for_walltime(self, requested_walltime, time=None):
        if time is None:
            time = self._scheduler.time
        time_updated = True
        while time_updated:
            time_updated = False
            # Search the earliest time when a slot for an allocation is
            # available
            for alloc in self._allocations:
                if alloc.start_time < time and alloc.end_time >= time:
                    time = alloc.end_time + 1
                    time_updated = True
            estimated_end_time = time + requested_walltime
            for alloc in self._allocations:
                if alloc.start_time > time and alloc.start_time < (estimated_end_time + 1):
                    time = alloc.end_time + 1
                    estimated_end_time = time + requested_walltime
                    time_updated = True
        return time

    def _update_pstate_change(self, pstate):
        self._old_pstate = self._pstate
        self._pstate = pstate
        self._pstate_update_in_progress = False
        self._pstate_update_request_necessary = False

    @property
    def pstate_update_in_progress(self):
        return self._pstate_update_in_progress

    @property
    def old_pstate(self):
        return self._old_pstate

    @property
    def pstate(self):
        return self._pstate

    @pstate.setter
    def pstate(self, newval):
        if not self.pstate_update_in_progress:
            self._old_pstate = self._pstate

        self._pstate_update_request_necessary = True
        self._pstate_update_in_progress = True

        self._pstate = newval

    def _do_change_state(self, scheduler):
        self._pstate_update_request_necessary = False
        scheduler._batsim.set_resource_state([self.id], self._new_state)

    def _do_add_allocation(self, allocation):
        # If time sharing is not enabled: check that allocations do not overlap
        if not self._scheduler.has_time_sharing:
            for alloc in self._allocations:
                if alloc.overlaps_with(allocation):
                    raise ValueError(
                        "Overlapping resource allocation while time-sharing is not enabled")
        self._allocations.add(allocation)

    def _do_remove_allocation(self, allocation):
        self._allocations.remove(allocation)

    def _do_allocate_allocation(self, allocation):
        pass

    def _do_free_allocation(self, allocation):
        self._allocations.remove(allocation)

    @property
    def resources(self):
        return [self]


class Resources(ObserveList):
    """Helper class implementing parts of the python list API to manage the resources.

       :param from_list: a list of `Resource` objects to be managed by this wrapper.
    """

    def __init__(self, *args, **kwargs):
        self._resource_map = {}
        super().__init__(*args, **kwargs)

    def __getitem__(self, items):
        if isinstance(items, slice):
            return self.create(self.all[items])
        else:
            return self._resource_map[items]

    def __delitem__(self, index):
        resource = self._resource_map[items]
        self.remove(resource)

    def __setitem__(self, index, element):
        raise ValueError("Cannot override a resource id")

    def _element_new(self, resource):
        if resource.id:
            self._resource_map[resource.id] = resource

    def _element_del(self, resource):
        if resource.id:
            del self._resource_map[resource.id]

    @property
    def resources(self):
        return self.all

    @property
    def free(self):
        return self.filter(free=True)

    def first_frame_for_walltime_combined(self,
            requested_walltime, time, min_matches, max_matches):
        time_updated = True
        while time_updated:
            time_updated = False
            found_resources = []
            for i, r in enumerate(self._data):
                new_time = r.first_frame_for_walltime(requested_walltime, time)
                if new_time != time:
                    if max_matches is None or len(found_resources) < max_matches:
                        time = new_time
                        time_updated = True
                        break
                    elif len(found_resources) >= max_matches:
                        break
                else:
                    found_resources.append(r)
                    if len(found_resources) >= max_matches:
                        break
        if len(found_resources) < min_matches:
            found_resources = []
        return time, self.create(found_resources)

    def find_sufficient_resources_for_job_with_earliest_start_time(self, job, *args, allow_future_allocations=False, **kwargs):
        has_time_sharing = job._scheduler.has_time_sharing
        resources = self.filter(*args, free=True, allocated=has_time_sharing, computing=has_time_sharing, **kwargs)

        start_time, found_resources = resources.first_frame_for_walltime_combined(job.requested_time, job._scheduler.time,
                job.requested_resources, job.requested_resources)

        if not allow_future_allocations and start_time != job._scheduler.time:
            found_resources = self.create()

        return found_resources, start_time

    def find_sufficient_resources_for_job(self, *args, **kwargs):
        return self.find_sufficient_resources_for_job_with_earliest_start_time(*args, **kwargs)[0]

    @property
    def allocated(self):
        return self.filter(allocated=True)

    @property
    def computing(self):
        return self.filter(computing=True)

    def filter(
            self,
            free=False,
            allocated=False,
            computing=False,
            num=None,
            **kwargs):
        """Filter the resources lists to search for resources.

        :param free: whether or not free resources should be returned.

        :param allocated: whether or not already allocated resources should be returned.

        :param computing: whether or not currently computing resources should be returned.
        """

        # Yield all resources if not filtered
        if not free and not allocated and not computing:
            free = True
            allocated = True
            computing = True

        filter_objects = []

        # Filter free or allocated resources
        def filter_free_or_allocated_resources(res):
            for r in res:
                if r.is_allocated:
                    if allocated:
                        yield r
                elif r.computing:
                    if computing:
                        yield r
                else:
                    if free:
                        yield r
        filter_objects.append(filter_free_or_allocated_resources)

        return self.create(filter_list(self._data,
                                       filter_objects,
                                       num=num,
                                       **kwargs))
