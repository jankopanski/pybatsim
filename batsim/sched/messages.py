"""
    batsim.sched.messages
    ~~~~~~~~~~~~~~~~~~~~~

    This module provides abstractions for managing the message buffers with messages from jobs.
"""

from .utils import DictWrapper, SafeIterList


class Message(DictWrapper):
    """Wraps a message received from a job (which is a dictionary) inside this
    object.
    Additionally to the fields of the message, the timestamp when the message was sent
    can also be accessed.

    :param timestamp: the timestamp when this message was sent

    :param message: the message (`dict`)
    """

    def __init__(self, timestamp, message):
        self.timestamp = timestamp
        super().__init__(message)

    def __str__(self):
        return (
            "<Message {}; data:{}>"
            .format(
                self.timestamp, super().__str__()))


class MessageBuffer(SafeIterList):
    """Buffer for all received messages.

    Currently it is just a python list where messages can be safely removed
    while iterating.
    """

    pass
