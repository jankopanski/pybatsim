"""
    batsim.sched.utils
    ~~~~~~~~~~~~~~~~~~

    Utility helpers used by other modules.

"""


class ObserveList:
    """Helper class implementing a filtered list."""

    def __init__(self, from_list=[]):
        self._data = list()
        self._data_set = set()
        for i in from_list:
            self.add(i)

    def _check_new_elem(self, element):
        return True

    def _element_new(self, element):
        pass

    def _element_del(self, element):
        pass

    def update_element(self, element):
        pass

    def __len__(self):
        return len(self._data)

    def __contains__(self, element):
        return element in self._data_set

    def __str__(self):
        return str(self._data)

    def add(self, element):
        if self._check_new_elem(element):
            self._data.append(element)
            self._data_set.add(element)
            self._element_new(element)

    def remove(self, element):
        self._data.remove(element)
        self._data_set.remove(element)
        self._element_del(element)

    def discard(self, element):
        try:
            self._data_set.remove(element)
            self._data.remove(element)
            self._element_del(element)
        except KeyError:
            pass

    def clear(self):
        for e in self._data:
            self._element_del(e)
        self._data.clear()
        self._data_set.clear()

    def __iter__(self):
        return iter(self._data)

    def __add__(self, other):
        """Concatenate two lists."""
        return self.create(set(self._data + other._data))

    def __str__(self):
        return str([str(entry) for entry in self._data])

    @property
    def data(self):
        return tuple(self._data)

    def apply(self, apply):
        """Apply a function to modify the list (e.g. sorting the list).

        :param apply: a function evaluating the result list.

        """
        return self.create(apply(self._data))

    def create(self, *args, **kwargs):
        return self.__class__(*args, **kwargs)

    def sorted(self, field_getter=None):
        return self.create(sorted(self._data, key=field_getter))


def filter_list(
        data,
        filter=[],
        cond=None,
        max=None,
        min=None,
        num=None,
        prefer_min=False,
        min_or_max=False):
    """Filter the list.

    :param filter: a list of generators through which the entries will be piped.

    :param cond: a function evaluating the entries and returns True or False whether or not the entry should be returned.

    :param max: the maximum number of returned entries.

    :param min: the minimum number of returned entries (if less entries are available no entries will be returned at all).

    :param num: the exact number of returned entries.

    :param prefer_min: return the minimum specified number of entries as soon as found.

    :param min_or_max: either return the minimum or maximum number but nothing between.

    """

    # If a concrete number of entries is requested do not yield less or
    # more
    if num:
        min = num
        max = num

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
    iterator = do_filter(data)
    while True:
        # Do not yield more entries than requested
        if max and num_elems >= max:
            break
        if min and prefer_min and num_elems == min:
            break

        try:
            elem = next(iterator)
        except StopIteration:
            break
        num_elems += 1
        result.append(elem)

    # Do not yield less entries than requested (better nothing than less)
    if min and len(result) < min:
        result = []
        num_elems = 0

    # Return only min elements if flag is set and num_elems is between min and
    # max
    if min_or_max and min and max and num_elems < max and num_elems > min:
        result = result[:min]

    # Construct a new list which can be filtered again
    return result
