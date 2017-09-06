"""
    batsim.sched.profiles
    ~~~~~~~~~~~~~~~~~~~~~

    This module provides an abstraction layer for job profiles.

    For dynamic profiles also `python dicts` may be used directly. However,
    by using the profile classes the library will take care of initialising the
    profile and setting sane default values for the profiles.

    When new profiles are implemented in Batsim a new profile in `Profiles` and a
    handler in `Profiles.profile_from_dict` should also be implemented to help
    creating dynamic versions of the new Batsim profile.
"""
from abc import ABCMeta, abstractmethod
from enum import Enum


class Profile(metaclass=ABCMeta):
    """A profile can be converted to a dictionary for being sent to Batsim."""

    def __init__(self, name=None):
        self._name = name

    @abstractmethod
    def to_dict(self):
        """Convert this profile to a dictionary."""
        pass

    @property
    def type(self):
        return self.__class__.type

    @property
    def name(self):
        return self._name

    def __call__(self):
        return self.to_dict()

    def copy(self):
        vals = {k: v for k, v in self.__dict__.items()
                if not k.startswith("_")}
        return self.__class__(name=self.name, **vals)

    def __str__(self):
        return (
            "<Profile {}; name:{} data:{}>"
            .format(
                self.type, self.name, self.to_dict()))


class Profiles(metaclass=ABCMeta):
    """Namespace for all built-in Batsim profiles."""

    @classmethod
    def profile_from_dict(cls, d, name=None):
        try:
            t = d["type"]
        except KeyError:
            return Unknown(data=data, name=name)

        profiles = [
            cls.Delay,
            cls.Parallel,
            cls.ParallelHomogeneous,
            cls.Smpi,
            cls.Sequence,
            cls.ParallelPFS,
            cls.DataStaging,
            cls.Send,
            cls.Receive
        ]

        for p in profiles:
            if t == p.type:
                return p.from_dict(d, name=name)
        return Unknown(data=data, type=t, name=name)

    class Unknown(Profile):
        """The profile was not recognized."""

        type = "unknown"

        def __init__(self, data, **kwargs):
            super().__init__(**kwargs)
            self.data = data

        def to_dict(self):
            return dict(self.data)

    class Delay(Profile):
        """Implementation of the Delay profile."""

        type = "delay"

        def __init__(self, delay=0, **kwargs):
            super().__init__(**kwargs)
            self.delay = delay

        @classmethod
        def from_dict(cls, dct, name=None):
            return cls(delay=dct["delay"], name=name)

        def to_dict(self):
            return {
                "type": self.type,
                "delay": self.delay
            }

    class Parallel(Profile):
        """Implementation of the MsgParallel profile."""

        type = "msg_par"

        def __init__(self, nbres=0, cpu=[], com=[], **kwargs):
            super().__init__(**kwargs)
            self.nbres = 0
            self.cpu = cpu
            self.com = com

        @classmethod
        def from_dict(cls, dct, name=None):
            return cls(nbres=dct["nb_res"],
                       cpu=dct["cpu"],
                       com=dct["com"],
                       name=name)

        def to_dict(self):
            return {
                "type": self.type,
                "nb_res": self.nbres,
                "cpu": self.cpu,
                "com": self.com
            }

    class ParallelHomogeneous(Profile):
        """Implementation of the MsgParallelHomogeneous profile."""

        type = "msg_par_hg"

        def __init__(self, cpu=0, com=0, **kwargs):
            super().__init__(**kwargs)
            self.cpu = cpu
            self.com = com

        @classmethod
        def from_dict(cls, dct, name=None):
            return cls(cpu=dct["cpu"],
                       com=dct["com"],
                       name=name)

        def to_dict(self):
            return {
                "type": self.type,
                "cpu": self.cpu,
                "com": self.com
            }

    class Smpi(Profile):
        """Implementation of the Smpi profile."""

        type = "smpi"

        def __init__(self, trace_file, **kwargs):
            super().__init__(**kwargs)
            self.trace_file = trace_file

        @classmethod
        def from_dict(cls, dct, name=None):
            return cls(trace_file=dct["trace_file"],
                       name=name)

        def to_dict(self):
            return {
                "type": self.type,
                "trace": self.trace_file
            }

    class Sequence(Profile):
        """Implementation of the Sequence profile."""

        type = "composed"

        def __init__(self, profiles=[], repeat=1, **kwargs):
            super().__init__(**kwargs)
            self.profiles = profiles
            self.repeat = repeat

        @classmethod
        def from_dict(cls, dct, name=None):
            return cls(profiles=dct["seq"],
                       repeat=dct.get("nb", 1),
                       name=name)

        def to_dict(self):
            return {
                "type": self.type,
                "nb": self.repeat,
                "seq": self.profiles
            }

    class ParallelPFS(Profile):
        """Implementation of the MsgParallelHomogeneousPFSMultipleTiers profile."""

        type = "msg_par_hg_pfs_tiers"

        class Direction(Enum):
            TO_STORAGE = 1
            FROM_STORAGE = 2

        class Host(Enum):
            HPST = 1
            LCST = 2

        def __init__(self, size,
                     direction=Direction.TO_STORAGE,
                     host=Host.LCST,
                     **kwargs):
            super().__init__(**kwargs)
            self.size = size
            self.direction = direction
            self.host = host

        @classmethod
        def from_dict(cls, dct, name=None):
            return cls(size=dct["size"],
                       direction=cls.Direction[dct["direction"]],
                       host=cls.Host[dct["host"]],
                       name=name)

        def to_dict(self):
            return {
                "type": self.type,
                "size": self.size,
                "direction": self.direction.name,
                "host": self.host.name
            }

    class DataStaging(Profile):
        """Implementation of the DataStaging profile."""

        type = "data_staging"

        class Direction(Enum):
            LCST_TO_HPST = 1
            HPST_TO_LCST = 2

        def __init__(self, size,
                     direction=Direction.LCST_TO_HPST,
                     **kwargs):
            super().__init__(**kwargs)
            self.size = size
            self.direction = direction

        @classmethod
        def from_dict(cls, dct, name=None):
            return cls(size=dct["size"],
                       direction=cls.Direction[dct["repeat"]],
                       name=name)

        def to_dict(self):
            return {
                "type": self.type,
                "size": self.size,
                "direction": self.direction.name
            }

    class Send(Profile):
        """Implementation of the SchedulerSend profile."""

        type = "send"

        def __init__(self, msg, sleeptime=None, **kwargs):
            super().__init__(**kwargs)
            self.msg = msg
            self.sleeptime = sleeptime

        @classmethod
        def from_dict(cls, dct, name=None):
            return cls(msg=dct["msg"],
                       sleeptime=dct.get("sleeptime", None),
                       name=name)

        def to_dict(self):
            if isinstance(self.msg, dict):
                msg = self.msg
            else:
                msg = self.msg.__dict__
            dct = {
                "type": self.type,
                "msg": msg,
            }
            if self.sleeptime is not None:
                dct["sleeptime"] = self.sleeptime
            return dct

    class Receive(Profile):
        """Implementation of the SchedulerRecv profile."""

        type = "recv"

        def __init__(
                self,
                regex,
                on_success=None,
                on_failure=None,
                on_timeout=None,
                polltime=None,
                **kwargs):
            super().__init__(**kwargs)
            self.regex = regex
            self.on_success = on_success
            self.on_failure = on_failure
            self.on_timeout = on_timeout
            self.polltime = polltime

        @classmethod
        def from_dict(cls, dct, name=None):
            return cls(regex=dct["regex"],
                       on_success=dct.get("on_success", None),
                       on_failure=dct.get("on_failure", None),
                       on_timeout=dct.get("on_timeout", None),
                       polltime=dct.get("polltime", None),
                       name=name)

        def to_dict(self):
            dct = {
                "type": self.type,
                "regex": self.regex,
            }
            if self.on_success is not None:
                dct["on_success"] = self.on_success
            if self.on_failure is not None:
                dct["on_failure"] = self.on_failure
            if self.on_timeout is not None:
                dct["on_timeout"] = self.on_timeout
            if self.polltime is not None:
                dct["polltime"] = self.polltime
            return dct
