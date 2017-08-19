"""
    batsim.sched.utils
    ~~~~~~~~~~~~~~~~~~

    Utility helpers used by other modules.

"""


class FilterList:
    """Helper class implementing a filtered list."""

    def __init__(self, from_list=[]):
        self._data = list(from_list)

    def __len__(self):
        return len(self._data)

    def __getitem__(self, items):
        return self._data[items]

    def __delitem__(self, index):
        del self._data[index]

    def __setitem__(self, index, element):
        self._data[index] = element

    def __str__(self):
        return str(self._data)

    def append(self, element):
        self._data.append(element)

    def remove(self, element):
        self._data.remove(element)

    def insert(self, index, element):
        self._data.insert(index, element)

    def __iter__(self):
        return iter(self._data)

    def __add__(self, other):
        """Concatenate two lists."""
        return self.create(set(self._data + other._data))

    @property
    def all(self):
        return tuple(self._data)

    def apply(self, apply):
        """Apply a function to modify the list (e.g. sorting the list).

        :param apply: a function evaluating the result list.

        """
        return self.create(apply(self._resources))

    def create(self, *args, **kwargs):
        return self.__class__(*args, **kwargs)

    def base_filter(
            self,
            filter=[],
            cond=None,
            limit=None,
            min=None,
            num=None):
        """Filter the list.

        :param filter: a list of generators through which the entries will be piped.

        :param cond: a function evaluating the entries and returns True or False whether or not the resource should be returned.

        :param limit: the maximum number of returned entries.

        :param min: the minimum number of returned entries (if less entries are available no entries will be returned at all).

        :param num: the exact number of returned entries.

        """

        # If a concrete number of entries is requested do not yield less or
        # more
        if num:
            min = num
            limit = num

        def filter_condition(res):
            if cond:
                for r in res:
                    if cond(r):
                        yield r
            else:
                for r in res:
                    yield r

        def do_filter(res):
            for gen in reversed(filter):
                res = gen(res)
            yield from filter_condition(res)

        result = []
        num_elems = 0
        for i in do_filter(self._data):
            # Do not yield more entries than requested
            if limit and num_elems >= limit:
                break
            num_elems += 1
            result.append(i)

        # Do not yield less entries than requested (better nothing than less)
        if min and len(result) < min:
            result = []

        # Construct a new list which can be filtered again
        return self.create(result)