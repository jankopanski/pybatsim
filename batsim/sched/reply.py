"""
    batsim.sched.reply
    ~~~~~~~~~~~~~~~~~~

    This module provides helper classes for replies sent by Batsim.

"""

from abc import ABCMeta


class BatsimReply(metaclass=ABCMeta):
    """Base class for all replies."""

    pass


class ConsumedEnergyReply(BatsimReply):
    """Reply after requesting the consumed energy from Batsim.

    :param consumed_energy: the consumed energy in the reply from Batsim
    """

    def __init__(self, consumed_energy):
        self.consumed_energy = consumed_energy
