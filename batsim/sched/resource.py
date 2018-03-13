"""
    batsim.sched.resource
    ~~~~~~~~~~~~~~~~~~~~~

    This module provides an abstraction around resources to keep track of allocations.

"""
from enum import Enum

from .utils import ObserveList, filter_list, ListView, build_filter, increment_float


class ResourceRequirement:
    """Resource requirements are additional requirements which have to be fulfilled
    when filtering for resources.

    Can be registered as handlers in the scheduler (`find_resource_handler`).
    Functions which search for resources for jobs make use of `ResourceRequirement`
    objects to specify the requirements based on properties set in the job.

    :param resources: the list of resources which can be used to fulfil the resource
                      requirement. Alternatively only a single resource object.

    :param min: the minimum number of resources which have to be selected

    :param max: the maximum number of resources which have to be selected

    :param filter: a resource filter function to limit the resource combinations

    :param num: when set this will set min and max
    """

    def __init__(self, resources, min=1, max=None, filter=None, num=None):
        if num is not None:
            num = abs(num)
            min = num
            max = num

        if not isinstance(resources, list):
            resources = [resources]

        self.resources = resources
        self.min = min
        self.max = max
        self.filter = filter


class Resource:
    """A resource is a limited resource managed by the scheduler.

    :param scheduler: the associated scheduler managing this resource.

    :param id: the id of this resource.

    :param name: the name of this resource.

    :param resources_list: the main resources list where this resource is contained

    :param time_sharing: whether this resource allows time sharing (if `None`, the
                         value set in Batsim will be used)
    """

    TIME_DELTA = 0.00000000001

    def __init__(
            self,
            scheduler,
            name,
            id=None,
            resources_list=None,
            time_sharing=None):
        self._scheduler = scheduler

        self._id = id
        self._name = name

        self._time_sharing = time_sharing

        self._resources_list = resources_list

        self._allocations = set()

    @property
    def id(self):
        """The id of this resource."""
        return self._id

    @property
    def time_sharing(self):
        """Whether this resource can be shared."""
        if self._time_sharing is None:
            return self._scheduler.has_time_sharing
        return self._time_sharing

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
    def active(self):
        """Whether or not this resource is currently active in some of its resources."""
        return self.num_active > 0

    @property
    def num_active(self):
        """Number of allocations in which this resource is currently active."""
        num = 0
        for alloc in self._allocations:
            if self in alloc.allocated_resources:
                num += 1
        return num

    @property
    def resources(self):
        """Returns a list containing only the resource (for compatibility with the `Resources` class)."""
        return [self]

    def find_first_time_to_fit_job(
            self,
            job,
            time=None,
            future_reservation=False):
        """Finds the first time after which the job can start.

        :param job: the job to find a time slot for

        :param time: the starting time after which a time slot is needed
        """
        raise NotImplementedError(
            "Implement this function in a concrete Resources class")

    def time_free(self, time=None):
        """Get time how long this resource is still free until the next reservation starts, `0`
        if the resource is allocated, and `Inf` if this resource has no allocations.

        Please note that this probably makes less sense for resources which allow time sharing
        since this function will only look for times where there is not a single allocation
        using this resource.

        :param time: the first time step to consider (default: the current time in the simulation)

        """
        result = float("Inf")

        if time is None:
            time = self._scheduler.time

        for alloc in self._allocations:
            # Allocation is currently active
            if alloc.start_time <= time and alloc.estimated_end_time >= time:
                result = 0
                break
            # Allocation starts after current time
            elif alloc.start_time > time:
                result = min(result, alloc.start_time)

        return result

    def _do_add_allocation(self, allocation):
        """Adds an allocation to this resource.

        It will be checked for overlaps (which are forbidden if time-sharing is not enabled).

        :param allocation: the allocation to be added
        """
        # If time sharing is not enabled: check that allocations do not overlap
        if not self.time_sharing:
            for alloc in self._allocations:
                if alloc.overlaps_with(allocation):
                    self._scheduler.fatal(
                        "Overlapping resource allocation in resource {resource} while time-sharing is not allowed, {own} overlaps with {other}",
                        resource=self,
                        own=alloc,
                        other=allocation)
        self._allocations.add(allocation)
        if self._resources_list:
            self._resources_list.update_element(self)

    def _do_remove_allocation(self, allocation):
        """Removes an allocation from this resource.

        :param allocation: the allocation to be removed.
        """
        self._allocations.remove(allocation)
        if self._resources_list:
            self._resources_list.update_element(self)

    def _do_allocate_allocation(self, allocation):
        """Hook which is called when an allocation becomes active.

        :param allocation: the allocation which becomes active
        """
        if self._resources_list:
            self._resources_list.update_element(self)
        self.on_allocate(allocation)

    def _do_free_allocation(self, allocation):
        """Hook which is called when an previously active allocation is freed.

        :param allocation: the allocation which is freed
        """
        self._allocations.remove(allocation)
        if self._resources_list:
            self._resources_list.update_element(self)
        self.on_free(allocation)

    def __str__(self):
        data = self.to_json_dict()
        return (
            "<{} {}; name={} allocs:{}>"
            .format(self.__class__.__name__, data["id"], data["name"],
                    data["allocations"]))

    def to_json_dict(self, recursive=True):
        """Returns a dict representation of this object.

        :param recursive: whether object references should be resolved
        """
        data = {
            "id": self.id,
            "name": self.name
        }
        if recursive:
            allocations = []

            for a in self.allocations:
                if recursive:
                    allocations.append(a.to_json_dict(recursive=False))
                else:
                    if a.job:
                        allocations.append(a.job.id)
                    else:
                        allocations.append([r.name for r in a.resources])
            data["allocations"] = allocations
        return data

    def on_allocate(self, allocation):
        pass

    def on_free(self, allocation):
        pass


class ComputeResource(Resource):
    """A compute resource is a machine managed by the resource manager (Batsim).

    :param state: the default state of this resource.

    :param properties: the dict of additional properties of this resource
    """

    class State(Enum):
        """The states of a machine."""
        SLEEPING = 0
        IDLE = 1
        COMPUTING = 2
        TRANSITING_FROM_SLEEPING_TO_COMPUTING = 3
        TRANSITING_FROM_COMPUTING_TO_SLEEPING = 4

    def __init__(self, *args, state, properties, **kwargs):
        # ComputeResources are only time_shared if Batsim allows this, which is
        # the default if set to False.
        super().__init__(*args, **kwargs)

        try:
            self._state = ComputeResource.State[(state or "").upper()]
        except KeyError:
            scheduler.fatal("Invalid machine state: {id}, {name}={state}",
                            id=id,
                            name=name,
                            state=state,
                            type="invalid_machine_state")
        self._properties = properties

        self._pstate = None
        self._old_pstate = None
        self._pstate_update_in_progress = False

    def find_first_time_to_fit_job(
            self,
            job,
            time=None,
            future_reservation=False):
        return self.find_first_time_to_fit_walltime(job.requested_time, time,
                                                    future_reservation)

    def find_first_time_to_fit_walltime(self, requested_walltime, time=None,
                                        future_reservation=False):
        """Finds the first time after which the requested walltime is available for a job start.

        :param requested_walltime: the size of the requested time slot

        :param time: the starting time after which a time slot is needed

        :param future_reservation: if future_reservation is set to True, it must not be
        guaranteed that the resource is already freed by Batsim at the given time.
        """
        present_time = self._scheduler.time
        if time is None:
            time = present_time
        start_time = time
        time_updated = True
        while time_updated:
            time_updated = False

            # Search the earliest time when a slot for an allocation is
            # available
            for alloc in self._allocations:
                # If the result should be found for the current scheduling time
                # (is_present) than not the estimated_end_time is used but the
                # real end_time (or infinity) because there could be allocations
                # in the current time which are not yet freed by Batsim (in case
                # of jobs getting killed after their walltime). This is due to
                # the implementation of killing jobs inside Batsim which is
                # implemented by using a new killer process.

                if alloc.end_time is None:
                    end_time = float("Inf")
                else:
                    end_time = alloc.estimated_end_time

                if alloc.start_time <= time and end_time == float(
                        "Inf") and not future_reservation and time <= present_time:
                    return None

                if alloc.start_time <= time and end_time >= time:
                    new_time = increment_float(
                        alloc.estimated_end_time,
                        Resource.TIME_DELTA,
                        until_changed=True)
                    if new_time < start_time:
                        new_time = start_time
                    if new_time > time:
                        time_updated = True
                        time = new_time
                        break
            # Check whether or not the full requested walltime fits into the
            # slot, otherwise move the slot at the end of the found conflicting
            # allocation and then repeat the loop.
            if not time_updated:
                estimated_end_time = increment_float(time, requested_walltime,
                                                     until_changed=True)
                estimated_end_time = increment_float(estimated_end_time,
                                                     Resource.TIME_DELTA,
                                                     until_changed=True)
                for alloc in self._allocations:
                    if alloc.start_time > time and alloc.start_time < (
                            estimated_end_time):
                        new_time = increment_float(
                            alloc.estimated_end_time,
                            Resource.TIME_DELTA,
                            until_changed=True)
                        if new_time < start_time:
                            new_time = start_time
                        if new_time > time:
                            time_updated = True
                            time = new_time
                            estimated_end_time = time + requested_walltime
                            break
        return time

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
        if self._resources_list:
            self._resources_list.update_element(self)

    def _update_pstate_change(self, pstate):
        """Update the pstate when called through a Batsim event.

        :param pstate: the new pstate
        """
        self._old_pstate = self._pstate
        self._pstate = pstate
        self._pstate_update_in_progress = False
        if self._resources_list:
            self._resources_list.update_element(self)


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
    def active(self):
        """The list of all active resources (resources which are allocated and active in an allocation)."""
        return self.filter(active=True)

    @property
    def compute(self):
        """The list of all compute resources (normal hosts)."""
        return self.filter(compute=True)

    @property
    def special(self):
        """The list of all special resources (managed by the scheduler logic)."""
        return self.filter(special=True)

    def __getitem__(self, item):
        """Returns either a slice of resources or returns a resource based on a given resource id."""
        if isinstance(item, slice):
            return self.create(self.all[item])
        else:
            return self._resource_map[item]

    def __delitem__(self, item):
        """Deletes a resource with the given resource id."""
        resource = self._resource_map[item]
        self.remove(resource)

    def _element_new(self, resource):
        if resource.id is not None:
            self._resource_map[resource.id] = resource

    def _element_del(self, resource):
        if resource.id is not None:
            del self._resource_map[resource.id]

    def find_first_time_and_resources_to_fit_walltime(
            self,
            job,
            time,
            min_matches=None,
            max_matches=None,
            filter=None,
            future_reservation=False):
        """Find sufficient resources and the earlierst start time to fit a job and its resource requirements.

        :param requested_walltime: the walltime which should fit in the allocation

        :param time: the earliest allowed start time of the allocation time are allowed

        :param min_matches: discard resources if less than `min_matches` were found

        :param max_matches: discard more resources than `max_matches`

        :param filter: the filter to be applied when a set of resources was found
        """
        def do_find(job, time, res, min_matches, max_matches, filter):
            # There are not enough resources available
            if min_matches is not None and len(res) < min_matches:
                return time, []

            while True:
                sufficient_resources_found = False
                found_resources = []
                earliest_time_available = None

                for r in res:
                    new_time = r.find_first_time_to_fit_job(
                        job, time, future_reservation)
                    if new_time is None:
                        continue
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
                            max_entries=max_matches, job=job,
                            current_time=time)(found_resources)

                        if found_resources and (
                                min_matches is None or len(found_resources) >= min_matches):
                            break
                        elif earliest_time_available is not None:
                            time = earliest_time_available
                        else:
                            return None, None
                    else:
                        break
                elif earliest_time_available:
                    if time != earliest_time_available:
                        time = earliest_time_available
                    else:
                        return None, None
                else:
                    if max_matches is not None:
                        found_resources = found_resources[:max_matches]
                    if min_matches is not None and len(
                            found_resources) < min_matches:
                        found_resources = []
                    return time, found_resources

            found_length = len(found_resources)

            if max_matches is not None:
                found_resources = found_resources[:max_matches]
                found_length = len(found_resources)
                assert found_length <= max_matches

            if min_matches is not None:
                assert found_length >= min_matches

            return time, found_resources

        while True:
            result = set()
            times_found = set()
            s_found_all = []
            is_valid = True

            new_time, found_res = do_find(job,
                                          time,
                                          self._data,
                                          min_matches,
                                          max_matches,
                                          filter)

            if new_time is None:
                return None, None

            if not found_res:
                is_valid = False

            if new_time < time:
                job._scheduler.fatal(
                    "Found time is before the current time: old={time_old}, new={time_new}",
                    time_old=time, time_new=new_time,
                    type="find_resource_failed_time_old")
            times_found.add(new_time)

            for h in job._scheduler.get_find_resource_handlers:
                reqs = h(job._scheduler, job)
                for r in reqs:
                    new_time2, s_found = do_find(job,
                                                 new_time,
                                                 r.resources,
                                                 r.min,
                                                 r.max,
                                                 r.filter)
                    if new_time2 is None:
                        return None, None

                    if not s_found:
                        is_valid = False

                    if new_time2 < new_time:
                        job._scheduler.fatal(
                            "Found time is before the current time: old={time_old}, new={time_new}",
                            time_old=new_time, time_new=new_time2,
                            type="find_resource_failed_time_old2")
                    s_found_all += s_found
                    times_found.add(new_time2)
                    if new_time2 != new_time:
                        break
            if len(times_found) == 1:
                if not is_valid:
                    return None, None

                return new_time, self.create(set(found_res + s_found_all))
            else:
                new_time = max(times_found)
                if time == new_time:
                    job._scheduler.fatal(
                        "Finding new resource failed. Time has not changed: old={time_old}, new={time_new}",
                        time_old=time, time_new=list(times_found),
                        type="find_resource_failed_time_not_changed")
                time = new_time

    def find_with_earliest_start_time(
            self, job, allow_future_allocations=False,
            filter=None, time=None):
        """Find sufficient resources and the earlierst start time for a given job.

        :param job: the job for which the start times and resources should be found

        :param allow_future_allocations: whether or not allocations starting after
                                         the current simulation time are allowed. If false only resources are returned which are guaranteed to be free in the present time.

        :param filter: the filter to be applied when a set of resources was found
        """
        if time is None:
            time = job._scheduler.time

        start_time, found_resources = self.find_first_time_and_resources_to_fit_walltime(job, max(
            time, job.submit_time), job.requested_resources, job.requested_resources, filter,
            allow_future_allocations)

        if not allow_future_allocations:
            if start_time is None:
                start_time = time
                found_resources = self.create()
            elif start_time != time:
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
            active=False,
            compute=False,
            special=False,
            **kwargs):
        """Filter the resources lists to search for resources.

        :param free: whether or not free resources should be returned.

        :param allocated: whether or not already allocated resources should be returned.

        :param active: whether or not currently active resources should be returned.

        :param compute: whether or not normal compute resources should be returned.

        :param special: whether or not special resources should be returned.
        """

        # Yield all resources if not filtered
        if not free and not allocated and not active and not compute and not special:
            free = True
            allocated = True
            active = True
            compute = True
            special = True

        filter_objects = []

        # Filter after the resource type
        def filter_free_or_allocated_resources(res, **kwargs):
            for r in res:
                if allocated:
                    if r.is_allocated:
                        yield r
                        continue
                if active:
                    if r.active:
                        yield r
                        continue
                if free:
                    if not r.is_allocated and not r.active:
                        yield r
                        continue
                if compute:
                    if isinstance(r, ComputeResource):
                        yield r
                        continue
                if special:
                    if not isinstance(r, ComputeResource):
                        yield r
        filter_objects.append(filter_free_or_allocated_resources)

        return self.create(filter_list(self._data,
                                       filter_objects,
                                       *args,
                                       **kwargs))
