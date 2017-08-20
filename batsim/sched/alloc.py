"""
    batsim.sched.allocs
    ~~~~~~~~~~~~~~~~~~~

    This module generalises the allocation of resources.
"""


class Allocation:

    def __init__(self, job):
        self._job = job
        self._resources = []

        self._allocated = False
        self._previously_allocated = False

    @property
    def job(self):
        return self._job

    @property
    def resources(self):
        return tuple(self._resources)

    @property
    def allocated(self):
        return self._allocated

    @property
    def previously_allocated(self):
        return self._previously_allocated

    def add_resource(self, resource):
        assert not self.allocated and not self.previously_allocated
        self._resources.append(resource)

    def remove_resource(self, resource):
        assert not self.allocated and not self.previously_allocated
        self._resources.remove(resource)

    def remove_all_resources(self):
        assert not self.allocated and not self.previously_allocated
        for r in self._resources[:]:
            self.remove_resource(r)

    def __len__(self):
        return len(self._resources)

    def __getitem__(self, items):
        return self._resources[items]

    def allocate(self):
        assert not self._previously_allocated and not self._allocated
        self._allocated = True

    def free(self):
        assert not self._previously_allocated and self._allocated

        self.job._allocation = None
        for r in self.resources:
            del r._allocations[self.job.id]

        self._allocated = False
        self._previously_allocated = True
