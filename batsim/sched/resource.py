"""
    batsim.sched.resource
    ~~~~~~~~~~~~~~~~~~~~~

    This module provides an abstraction around resources to keep track of allocations.

"""
from enum import Enum

from .utils import ObserveList, filter_list, ListView, build_filter


class Resource:
    """A resource is a machine managed by the resource manager.

    :param scheduler: the associated scheduler managing this resource.

    :param id: the id of this resource.

    :param name: the name of this resource.

    :param state: the default state of this resource.

    :param properties: the dict of additional properties of this resource

    :param resources_list: the main resources list where this resource is contained
    """

    class State(Enum):
        """The states of a machine."""
        SLEEPING = 0
        IDLE = 1
        COMPUTING = 2
        TRANSITING_FROM_SLEEPING_TO_COMPUTING = 3
        TRANSITING_FROM_COMPUTING_TO_SLEEPING = 4

    def __init__(self, scheduler, id, name, state, properties, resources_list):
        self._scheduler = scheduler
        self._id = id
        self._name = name

        self._resources_list = resources_list

        try:
            self._state = Resource.State[(state or "").upper()]
        except KeyError:
            scheduler.fatal("Invalid machine state: {id}, {name}={state}",
                            id=id,
                            name=name,
                            state=state,
                            type="invalid_machine_state")
        self._properties = properties

        self._allocations = set()

        self._pstate = None
        self._old_pstate = None
        self._pstate_update_in_progress = False

    @property
    def id(self):
        """The id of this resource."""
        return self._id

    @property
    def name(self):
        """The name of this resource."""
        return self._name

    @property
    def is_allocated(self):
        """Whether or not this resource is currently allocated."""
        return bool(self._allocations)

    @property
    def allocations(self):
        """A copy of the allocations (current and future) where this resource is part of."""
        return ListView(self._allocations)

    @property
    def computing(self):
        """Whether or not this resource is currently computing in some of its resources."""
        for alloc in self._allocations:
            if self in alloc.allocated_resources:
                return True
        return False

    @property
    def pstate_update_in_progress(self):
        """Whether or not a pstate update is currently in progress (sent to Batsim but still pending)."""
        return self._pstate_update_in_progress

    @property
    def old_pstate(self):
        """Returns the previous pstate."""
        return self._old_pstate

    @property
    def pstate(self):
        """Returns the current pstate."""
        return self._pstate

    @pstate.setter
    def pstate(self, newval):
        if not self.pstate_update_in_progress:
            self._old_pstate = self._pstate

        scheduler._batsim.set_resource_state([self.id], self._pstate)

        self._pstate_update_in_progress = True

        self._pstate = newval
        self._resources_list.update_element(self)

    @property
    def resources(self):
        """Returns a list containing only the resource (for compatibility with the `Resources` class)."""
        return [self]

    def time_free(self, time=None):
        """Get time how long this resource is still free until the next reservation starts, `0` if the resource is allocated, and
        `Inf` if this resource has no allocations.

        :param time: the first time step to consider (default: the current time in the simulation)
        """
        result = float("Inf")

        if time is None:
            time = self._scheduler.time

        for alloc in self._allocations:
            # Allocation is currently active
            if alloc.start_time <= time and alloc.end_time >= time:
                result = 0
                break
            # Allocation starts after current time
            elif alloc.start_time > time:
                result = min(result, alloc.start_time)

        return result

    def find_first_time_to_fit_walltime(self, requested_walltime, time=None):
        """Finds the first time after which the requested walltime is available for a job start.

        :param requested_walltime: the size of the requested time slot

        :param time: the starting time after which a time slot is needed
        """
        if time is None:
            time = self._scheduler.time
        time_updated = True
        while time_updated:
            time_updated = False
            # Search the earliest time when a slot for an allocation is
            # available
            for alloc in self._allocations:
                if alloc.start_time <= time and alloc.end_time >= time:
                    time = alloc.end_time + 0.000001
                    time_updated = True
            # Check whether or not the full requested walltime fits into the
            # slot, otherwise move the slot at the end of the found conflicting
            # allocation and then repeat the loop.
            estimated_end_time = time + requested_walltime
            for alloc in self._allocations:
                if alloc.start_time > time and alloc.start_time < (
                        estimated_end_time + 0.000001):
                    time = alloc.end_time + 0.000001
                    estimated_end_time = time + requested_walltime
                    time_updated = True
        return time

    def _update_pstate_change(self, pstate):
        """Update the pstate when called through a Batsim event.

        :param pstate: the new pstate
        """
        self._old_pstate = self._pstate
        self._pstate = pstate
        self._pstate_update_in_progress = False
        self._resources_list.update_element(self)

    def _do_add_allocation(self, allocation):
        """Adds an allocation to this resource.

        It will be checked for overlaps (which are forbidden if time-sharing is not enabled).

        :param allocation: the allocation to be added
        """
        # If time sharing is not enabled: check that allocations do not overlap
        if not self._scheduler.has_time_sharing:
            for alloc in self._allocations:
                if alloc.overlaps_with(allocation):
                    self._scheduler.fatal(
                        "Overlapping resource allocation while time-sharing is not enabled, {own} overlaps with {other}",
                        own=alloc,
                        other=allocation)
        self._allocations.add(allocation)
        self._resources_list.update_element(self)

    def _do_remove_allocation(self, allocation):
        """Removes an allocation from this resource.

        :param allocation: the allocation to be removed.
        """
        self._allocations.remove(allocation)
        self._resources_list.update_element(self)

    def _do_allocate_allocation(self, allocation):
        """Hook which is called when an allocation becomes active.

        :param allocation: the allocation which becomes active
        """
        self._resources_list.update_element(self)

    def _do_free_allocation(self, allocation):
        """Hook which is called when an previously active allocation is freed.

        :param allocation: the allocation which is freed
        """
        self._allocations.remove(allocation)
        self._resources_list.update_element(self)

    def __str__(self):
        return (
            "<Resource {}; name:{} pstate:{} allocs:{}>"
            .format(
                self.id, self.name, self.pstate,
                [str(a) for a in self.allocations]))


class Resources(ObserveList):
    """Helper class implementing parts of the python list API to manage the resources.

       :param from_list: a list of `Resource` objects to be managed by this wrapper.
    """

    def __init__(self, *args, **kwargs):
        self._resource_map = {}
        super().__init__(*args, **kwargs)

    @property
    def resources(self):
        """The list of all resources in this resource object."""
        return self.all

    @property
    def free(self):
        """The list of all free resources."""
        return self.filter(free=True)

    @property
    def allocated(self):
        """The list of all allocated resources."""
        return self.filter(allocated=True)

    @property
    def computing(self):
        """The list of all computing resources (resources which are allocated and active in an allocation)."""
        return self.filter(computing=True)

    def __getitem__(self, items):
        """Returns either a slice of resources or returns a resource based on a given resource id."""
        if isinstance(items, slice):
            return self.create(self.all[items])
        else:
            return self._resource_map[items]

    def __delitem__(self, index):
        """Deletes a resource with the given resource id."""
        resource = self._resource_map[items]
        self.remove(resource)

    def _element_new(self, resource):
        if resource.id:
            self._resource_map[resource.id] = resource

    def _element_del(self, resource):
        if resource.id:
            del self._resource_map[resource.id]

    def find_first_time_and_resources_to_fit_walltime(
            self,
            requested_walltime,
            time,
            min_matches=None,
            max_matches=None,
            time_sharing=False,
            filter=None):
        """Find sufficient resources and the earlierst start time to fit a walltime and resource requirements.

        :param requested_walltime: the walltime which should fit in the allocation

        :param time: the earliest allowed start time of the allocation time are allowed

        :param min_matches: discard resources if less than `min_matches` were found

        :param max_matches: discard more resources than `max_matches`

        :param filter: the filter to be applied when a set of resources was found
        """
        if time_sharing:
            raise ValueError(
                "Finding allocations when time sharing is enabled is currently not implemented")

        # There are not enough resources available
        if min_matches is not None and len(self) < min_matches:
            return time, self.create()

        while True:
            sufficient_resources_found = False
            found_resources = []
            earliest_time_available = None
            time_changed = False
            for r in self._data:
                new_time = r.find_first_time_to_fit_walltime(
                    requested_walltime, time)

                if new_time == time:
                    found_resources.append(r)
                    if min_matches is None or len(
                            found_resources) >= min_matches:
                        sufficient_resources_found = True
                else:
                    if earliest_time_available is None:
                        earliest_time_available = new_time
                    else:
                        earliest_time_available = min(
                            earliest_time_available, new_time)
            if sufficient_resources_found:
                if filter:
                    found_resources = build_filter(
                        filter, min_entries=min_matches,
                        max_entries=max_matches, walltime=requested_walltime,
                        current_time=time)(found_resources)

                    if found_resources and (
                            min_matches is None or len(found_resources) >= min_matches):
                        break
                    else:
                        time = earliest_time_available
                else:
                    break
            elif earliest_time_available:
                time = earliest_time_available

        found_length = len(found_resources)

        if max_matches is not None:
            found_resources = found_resources[:max_matches]
            found_length = len(found_resources)
            assert found_length <= max_matches

        if min_matches is not None:
            assert found_length >= min_matches

        return time, self.create(found_resources)

    def find_with_earliest_start_time(
            self, job, allow_future_allocations=False,
            filter=None):
        """Find sufficient resources and the earlierst start time for a given job.

        :param job: the job for which the start times and resources should be found

        :param allow_future_allocations: whether or not allocations starting after
                                         the current simulation time are allowed

        :param filter: the filter to be applied when a set of resources was found
        """
        start_time, found_resources = self.find_first_time_and_resources_to_fit_walltime(
            job.requested_time, max(job._scheduler.time, job.submit_time), job.requested_resources, job.requested_resources,
            job._scheduler.has_time_sharing,
            filter)

        if not allow_future_allocations and start_time != job._scheduler.time:
            found_resources = self.create()

        return start_time, found_resources

    def find_sufficient_resources_for_job(self, *args, **kwargs):
        """Find sufficient resources for a given job.

        For supported arguments see `find_with_earliest_start_time`.

        """
        return self.find_with_earliest_start_time(
            *args, **kwargs)[1]

    def filter(
            self,
            *args,
            free=False,
            allocated=False,
            computing=False,
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

        # Filter after the resource type
        def filter_free_or_allocated_resources(res, **kwargs):
            for r in res:
                if allocated:
                    if r.is_allocated:
                        yield j
                        continue
                if computing:
                    if r.computing:
                        yield j
                        continue
                if free:
                    if not r.is_allocated and not r.computing:
                        yield r
        filter_objects.append(filter_free_or_allocated_resources)

        return self.create(filter_list(self._data,
                                       filter_objects,
                                       *args,
                                       **kwargs))
