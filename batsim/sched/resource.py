
class Resource:

    def __init__(self, scheduler, batsim_id=-1):
        self._scheduler = scheduler
        self._id = batsim_id
        self._allocated_by = []
        self._previously_allocated_by = []
        self._computing = False

    @property
    def id(self):
        return self._id

    @property
    def is_allocated(self):
        return bool(self._allocated_by)

    @property
    def allocated_by(self):
        return tuple(self._allocated_by)

    @property
    def previously_allocated_by(self):
        return tuple(self._previously_allocated_by)

    def allocate(self, job, recursive_call=False):
        assert not self.is_allocated, "Node sharing is currently not allowed"

        if not recursive_call:
            job.reserve(self, recursive_call=True)
        self._allocated_by.append(job)

    def free(self, job, recursive_call=False):
        assert job in self._allocated_by, "Job is not allocated on this resource"

        if not recursive_call:
            job.free(self, recursive_call=True)

        self._previously_allocated_by.append((self._scheduler.time, job))
        self._allocated_by.remove(job)

        if not self.is_allocated:
            self.computing = False

    @property
    def computing(self):
        return self._computing

    @computing.setter
    def computing(self, value):
        self._computing = value

    @property
    def resources(self):
        return [self]


class Resources:

    def __init__(self, from_list=[]):
        self._resources = list(from_list)

    def allocate(self, job, recursive_call=False):
        for r in self._resources:
            r.allocate(job, recursive_call=recursive_call)

    def free(self, job, recursive_call=False):
        for r in self._resources:
            r.free(job, recursive_call=recursive_call)

    @property
    def resources(self):
        return tuple(self._resources)

    def __add__(self, other):
        return Resources(set(self._resources + other._resources))

    def filter(
            self,
            cond=None,
            sort=None,
            free=False,
            allocated=False,
            limit=None,
            min=None,
            num=None,
            for_job=None):
        nr = []

        # Pre-defined filter to find resources for a job submission
        if for_job is not None:
            free = True
            allocated = False
            num = for_job.requested_resources

        # Yield all resources if not filtered
        if not free and not allocated:
            free = True
            allocated = True

        # If a concrete number of resources is requested do not yield less or
        # more
        if num:
            min = num
            limit = num

        # Filter free or allocated resources
        for r in self._resources:
            if r.is_allocated:
                if allocated:
                    nr.append(r)
            else:
                if free:
                    nr.append(r)

        # Filter applying a given condition
        if cond:
            nr2 = nr
            nr = []
            for r in nr2:
                if cond(r):
                    nr.append(r)

        # Sort the resources if a function was given. Resources which should be
        # preferred to be chosen can be sorted to the front of the list.
        if sort:
            nr = sort(nr)

        # Do not yield more resources than requested
        if limit:
            nr = nr[:limit]

        # Do not yield less resources than requested (better nothing than less)
        if min and len(nr) < min:
            nr = []

        # Construct a new resources list which can be filtered again
        return Resources(nr)

    def __len__(self):
        return len(self._resources)

    def __getitem__(self, items):
        return self._resources[items]

    def __delitem__(self, index):
        del self._resources[index]

    def __setitem__(self, index, element):
        self._resources[index] = element

    def __str__(self):
        return str(self._resources)

    def append(self, element):
        self._resources.append(element)

    def remove(self, element):
        self._resources.remove(element)

    def insert(self, index, element):
        self._resources.insert(index, element)

    def __iter__(self):
        return iter(self._resources)
