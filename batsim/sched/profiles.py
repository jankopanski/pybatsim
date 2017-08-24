"""
    batsim.sched.profiles
    ~~~~~~~~~~~~~~~~~~~~~

    This module provides an abstraction layer for job profiles.

"""
from abc import ABCMeta, abstractmethod
from enum import Enum


class Profile(metaclass=ABCMeta):
    """A profile is an arbitrary object which has to be converted to a dictionary
    afterwards in order to send it to the Batsim instance.
    """

    @abstractmethod
    def to_dict(self, scheduler):
        """Convert this profile to a dictionary."""
        pass

    def __call__(self, scheduler):
        return self.to_dict(scheduler)


class Profiles(metaclass=ABCMeta):
    """Namespace for all built-in Batsim profiles."""

    class Delay(Profile):
        """Implementation of the Delay profile."""

        def __init__(self, delay=0):
            self.delay = delay

        def to_dict(self):
            return {
                "type": "delay",
                "delay": self.delay
            }

    class Parallel(Profile):
        """Implementation of the MsgParallel profile."""

        def __init__(self, nbres=0, cpu=[], com=[]):
            self.nbres = 0
            self.cpu = cpu
            self.com = com

        def to_dict(self):
            return {
                "type": "msg_par",
                "nb_res": self.nbres,
                "cpu": self.cpu,
                "com": self.com
            }

    class ParallelHomogeneous(Profile):
        """Implementation of the MsgParallelHomogeneous profile."""

        def __init__(self, cpu=0, com=0):
            self.cpu = cpu
            self.com = com

        def to_dict(self):
            return {
                "type": "msg_par_hg",
                "cpu": self.cpu,
                "com": self.com
            }

    class Smpi(Profile):
        """Implementation of the Smpi profile."""

        def __init__(self, trace_file):
            self.trace_file = trace_file

        def to_dict(self):
            return {
                "type": "composed",
                "trace": self.trace_file
            }

    class Sequence(Profile):
        """Implementation of the Sequence profile."""

        def __init__(self, profiles=[], repeat=1):
            self.profiles = profiles
            self.repeat = repeat

        def to_dict(self):
            return {
                "type": "composed",
                "nb": self.repeat,
                "seq": self.profiles
            }

    class ParallelPFS(Profile):
        """Implementation of the MsgParallelHomogeneousPFSMultipleTiers profile."""

        class Direction(Enum):
            TO_STORAGE = 1
            FROM_STORAGE = 2

        class Host(Enum):
            HPST = 1
            LCST = 2

        def __init__(self, size,
                     direction=Direction.TO_STORAGE,
                     host=Host.LCST):
            self.size = size
            self.direction = direction
            self.host = host

        def to_dict(self):
            return {
                "type": "msg_par_hg_pfs_tiers",
                "size": self.size,
                "direction": self.direction.name,
                "host": self.host.name
            }

    class DataStaging(Profile):
        """Implementation of the DataStaging profile."""

        class Direction(Enum):
            LCST_TO_HPST = 1
            HPST_TO_LCST = 2

        def __init__(self, size,
                     direction=Direction.LCST_TO_HPST):
            self.size = size
            self.direction = direction

        def to_dict(self):
            return {
                "type": "data_staging",
                "size": self.size,
                "direction": self.direction.name
            }
