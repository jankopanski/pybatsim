"""
    batsim.sched.utils
    ~~~~~~~~~~~~~~~~~~

    Utility helpers used by other modules.

"""


class ObserveList:
    """Helper class implementing a filtered list."""

    def __init__(self, from_list=[]):
        self._data = list(from_list)
        for i in from_list:
            self._element_new(i)

    def _element_new(self, element):
        pass

    def _element_del(self, element):
        pass

    def update_element(self, element):
        pass

    def __len__(self):
        return len(self._data)

    def __getitem__(self, items):
        return self._data[items]

    def __delitem__(self, index):
        has_elem = False
        try:
            oldelem = self._data[index]
            has_elem = True
        except KeyError:
            pass
        del self._data[index]
        if has_elem:
            self._element_del(oldelem)

    def __setitem__(self, index, element):
        has_elem = False
        try:
            oldelem = self._data[index]
            has_elem = True
        except KeyError:
            pass
        self._data[index] = element
        if has_elem:
            self._element_del(oldelem)
        self._element_new(element)

    def __str__(self):
        return str(self._data)

    def append(self, element):
        self._data.append(element)
        self._element_new(element)

    def remove(self, element):
        self._data.remove(element)
        self._element_del(element)

    def insert(self, index, element):
        self._data.insert(index, element)
        self._element_new(element)

    def __iter__(self):
        return iter(self._data)

    def __add__(self, other):
        """Concatenate two lists."""
        return self.create(set(self._data + other._data))

    def __str__(self):
        return str([str(entry) for entry in self._data])

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


def filter_list(
        data,
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
    for i in do_filter(data):
        # Do not yield more entries than requested
        if limit and num_elems >= limit:
            break
        num_elems += 1
        result.append(i)

    # Do not yield less entries than requested (better nothing than less)
    if min and len(result) < min:
        result = []

    # Construct a new list which can be filtered again
    return result
