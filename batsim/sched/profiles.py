"""
    batsim.sched.profiles
    ~~~~~~~~~~~~~~~~~~~~~

    This module provides an abstraction layer for job profiles.

"""
from abc import ABCMeta, abstractmethod


class Profile(metaclass=ABCMeta):
    """A profile is an arbitrary object which has to be converted to a dictionary
    afterwards in order to send it to the Batsim instance.
    """

    @abstractmethod
    def to_dict(self):
        """Convert this profile to a dictionary."""
        pass

    def __call__(self):
        return self.to_dict()


class Profiles(metaclass=ABCMeta):
    """Namespace for all built-in Batsim profiles."""

    class Delay(Profile):
        """Implementation of the delay profile."""

        def __init__(self, time):
            self.time = time

        def to_dict(self):
            return {
                "type": "delay",
                "delay": self.time
            }
