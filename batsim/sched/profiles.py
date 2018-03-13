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
import json


class Profile(metaclass=ABCMeta):
    """A profile can be converted to a dictionary for being sent to Batsim."""

    def __init__(self, name=None, ret=0):
        self._name = name
        self._ret = ret

    @abstractmethod
    def to_dict(self, embed_references=False):
        """Convert this profile to a dictionary.

        :param embed_references: whether or not references to other profiles should
                                 be embedded (only works if references are given as
                                 `Profile` objects).
        """
        pass

    @property
    def type(self):
        return self.__class__.type

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def ret(self):
        return self._ret

    @ret.setter
    def ret(self, value):
        self._ret = value

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
                self.type, self.name, self.to_dict(embed_references=True)))

    def __eq__(self, other):
        if isinstance(other, Profile):
            return json.dumps(
                self.to_dict(
                    embed_references=True),
                sort_keys=True) \
                == json.dumps(
                other.to_dict(
                    embed_references=True),
                sort_keys=True)
        else:
            return False

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(
            json.dumps(
                self.to_dict(
                    embed_references=True),
                sort_keys=True))

    def get_additional_profiles(self):
        """Return the additional profile objects which are referenced in this profile."""
        return []

    def replace_additional_profiles(self, map):
        """Replace additional profile objects with the profiles from the map if the map contains the same profile already."""
        pass

    def _convert_profile_reference(self, profile, embed=True):
        """Returns the reference to the given profile (usually the name).

        :param profile: the profile to be referenced

        :param embed: whether the profile should be directly included (no reference)

        """
        if isinstance(profile, Profile):
            if embed:
                return profile.to_dict(embed_references=embed)
            elif profile.name is None:
                raise ValueError(
                    "Profile reference has no name: {}".format(profile))
            return profile.name
        else:
            return profile


class Profiles(metaclass=ABCMeta):
    """Namespace for all built-in Batsim profiles."""

    @classmethod
    def profile_from_dict(cls, d, name=None):
        try:
            t = d["type"]
        except KeyError:
            return Profiles.Unknown(data=d, name=name)

        profiles = [
            cls.Delay,
            cls.Parallel,
            cls.ParallelHomogeneous,
            cls.ParallelHomogeneousTotal,
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
        return Profiles.Unknown(data=d, type=t, name=name)

    class Unknown(Profile):
        """The profile was not recognized."""

        type = "unknown"

        def __init__(self, data, **kwargs):
            super().__init__(**kwargs)
            self.data = data

        def to_dict(self, embed_references=False):
            return dict(self.data)

    class Delay(Profile):
        """Implementation of the Delay profile."""

        type = "delay"

        def __init__(self, delay=0, **kwargs):
            super().__init__(**kwargs)
            self.delay = delay

        @classmethod
        def from_dict(cls, dct, name=None):
            return cls(delay=dct["delay"],
                       ret=dct.get("ret", 0),
                       name=name)

        def to_dict(self, embed_references=False):
            return {
                "type": self.type,
                "delay": self.delay,
                "ret": self.ret,
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
            return cls(cpu=dct["cpu"],
                       com=dct["com"],
                       ret=dct.get("ret", 0),
                       name=name)

        def to_dict(self, embed_references=False):
            return {
                "type": self.type,
                "cpu": self.cpu,
                "com": self.com,
                "ret": self.ret,
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
                       ret=dct.get("ret", 0),
                       name=name)

        def to_dict(self, embed_references=False):
            return {
                "type": self.type,
                "cpu": self.cpu,
                "com": self.com,
                "ret": self.ret,
            }

    class ParallelHomogeneousTotal(ParallelHomogeneous):
        """Implementation of the MsgParallelHomogeneousTotal profile."""

        type = "msg_par_hg_tot"


    class Smpi(Profile):
        """Implementation of the Smpi profile."""

        type = "smpi"

        def __init__(self, trace_file, **kwargs):
            super().__init__(**kwargs)
            self.trace_file = trace_file

        @classmethod
        def from_dict(cls, dct, name=None):
            return cls(trace_file=dct["trace_file"],
                       ret=dct.get("ret", 0),
                       name=name)

        def to_dict(self, embed_references=False):
            return {
                "type": self.type,
                "trace": self.trace_file,
                "ret": self.ret,
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
                       ret=dct.get("ret", 0),
                       name=name)

        def to_dict(self, embed_references=False):
            return {
                "type": self.type,
                "nb": self.repeat,
                "ret": self.ret,
                "seq": [
                    self._convert_profile_reference(
                        profile,
                        embed_references) for profile in self.profiles]}

        def get_additional_profiles(self):
            return self.profiles

        def replace_additional_profiles(self, map):
            self.profiles = [map.get(profile, profile)
                             for profile in self.profiles]

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
                       ret=dct.get("ret", 0),
                       name=name)

        def to_dict(self, embed_references=False):
            return {
                "type": self.type,
                "size": self.size,
                "direction": self.direction.name.lower(),
                "host": self.host.name,
                "ret": self.ret,
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
                       ret=dct.get("ret", 0),
                       name=name)

        def to_dict(self, embed_references=False):
            return {
                "type": self.type,
                "size": self.size,
                "direction": self.direction.name.lower(),
                "ret": self.ret,
            }

    class Send(Profile):
        """Implementation of the SchedulerSend profile."""

        type = "send"

        def __init__(self, msg, sleeptime=None, **kwargs):
            assert isinstance(
                msg, dict), "Batsim expects a json object as a message"
            super().__init__(**kwargs)
            self.msg = msg
            self.sleeptime = sleeptime

        @classmethod
        def from_dict(cls, dct, name=None):
            return cls(msg=dct["msg"],
                       sleeptime=dct.get("sleeptime", None),
                       ret=dct.get("ret", 0),
                       name=name)

        def to_dict(self, embed_references=False):
            if isinstance(self.msg, dict):
                msg = self.msg
            else:
                msg = self.msg.__dict__
            dct = {
                "type": self.type,
                "msg": msg,
                "ret": self.ret,
            }
            if self.sleeptime is not None:
                dct["sleeptime"] = self.sleeptime
            return dct

    class Receive(Profile):
        """Implementation of the SchedulerRecv profile."""

        type = "recv"

        def __init__(
                self,
                regex=".*",
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
                       on_success=dct.get("success", None),
                       on_failure=dct.get("failure", None),
                       on_timeout=dct.get("timeout", None),
                       polltime=dct.get("polltime", None),
                       ret=dct.get("ret", 0),
                       name=name)

        def to_dict(self, embed_references=False):
            dct = {
                "type": self.type,
                "regex": self.regex,
                "ret": self.ret,
            }
            if self.on_success is not None:
                dct["success"] = self._convert_profile_reference(
                    self.on_success, embed_references)
            if self.on_failure is not None:
                dct["failure"] = self._convert_profile_reference(
                    self.on_failure, embed_references)
            if self.on_timeout is not None:
                dct["timeout"] = self._convert_profile_reference(
                    self.on_timeout, embed_references)
            if self.polltime is not None:
                dct["polltime"] = self.polltime
            return dct

        def get_additional_profiles(self):
            profiles = []
            if self.on_success is not None:
                profiles.append(self.on_success)
            if self.on_failure is not None:
                profiles.append(self.on_failure)
            if self.on_timeout is not None:
                profiles.append(self.on_timeout)
            return profiles

        def replace_additional_profiles(self, map):
            if self.on_success is not None:
                self.on_success = map.get(self.on_success, self.on_success)
            if self.on_failure is not None:
                self.on_failure = map.get(self.on_failure, self.on_failure)
            if self.on_timeout is not None:
                self.on_timeout = map.get(self.on_timeout, self.on_timeout)

        @classmethod
        def if_recv(
                cls,
                regex,
                on_success=None,
                on_failure=None,
                on_timeout=None,
                polltime=None,
                **kwargs):
            """Returns a `Profiles.Receive` instance generated from the profile
            generator functions.

            :param regex: the regex used in the receive

            :param on_success: the function which will be converted to a profile
                               for the on_success handler

            :param on_failure: the function which will be converted to a profile
                               for the on_failure handler

            :param on_timeout: the function which will be converted to a profile
                               for the on_timeout handler

            :param kwargs: additional arguments forwarded to the `Receive` profile.
            """
            on_success_profile = Profiles.profile_from_generator(
                on_success) if on_success else None
            on_failure_profile = Profiles.profile_from_generator(
                on_failure) if on_failure else None
            on_timeout_profile = Profiles.profile_from_generator(
                on_timeout) if on_timeout else None
            return cls(regex,
                       on_success_profile,
                       on_failure_profile,
                       on_timeout_profile,
                       polltime,
                       **kwargs)

    @classmethod
    def profile_from_generator(cls, func, *args, **kwargs):
        """Constructs a profile based on executing a generator function.

        The generator should yield `Profile` objects. If more than one profile is
        yielded a `Profiles.Sequence` profile is constructed, otherwise the profile
        is directly used. The generator may also yield lists of profiles.

        :param func: the generation which should yield profiles

        :param args: arguments forwarded to the call of `func`

        :param kwargs: arguments forwarded to the call of `func`

        """
        profiles = []
        func_iter = iter(func(*args, **kwargs))
        to_process = []
        while True:
            if not to_process:
                try:
                    to_process.append(next(func_iter))
                except StopIteration:
                    break
            p = to_process.pop(0)
            if isinstance(p, list):
                profiles += p
            elif isinstance(p, Profile):
                profiles.append(p)
            else:
                raise ValueError("Unknown profile element: {}".format(p))

        if len(profiles) > 1:
            return Profiles.Sequence(profiles)
        elif len(profiles) == 1:
            return profiles[0]
        else:
            raise ValueError("Profile sequence contains no elements")
