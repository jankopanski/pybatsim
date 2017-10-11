"""
    batsim.sched.events
    ~~~~~~~~~~~~~~~~~~~

    This module provides handling of scheduling events.
"""

import logging
import json
import csv

from .logging import Logger


class LoggingEvent:
    """Class for storing data about events triggered by the scheduler.

    :param time: the simulation time when the event occurred

    :param level: the importance level of the event

    :param open_jobs: the number of open jobs

    :param processed_jobs: the number of processed jobs (completed, killed, etc.)

    :param msg: the actual message of the event

    :param type: the type of the event (`str`)

    :param data: additional data attached to the event (`dict`)
    """

    def __init__(
            self,
            time,
            level,
            open_jobs,
            processed_jobs,
            msg,
            type,
            data):
        self.time = time
        self.open_jobs = open_jobs
        self.processed_jobs = processed_jobs
        self.level = level
        self.msg = msg
        self.type = type
        self.data = data

    def to_message(self):
        """Returns a human readable message presentation of this event."""
        return "[{:.6f}] {}/{} <{}> ({})".format(
            self.time, self.processed_jobs, self.open_jobs,
            self.type, self.msg)

    def __str__(self):
        data = []
        for k, v in self.data.items():
            try:
                k = json.dumps(k, default=lambda o: o.__dict__)
            except (AttributeError, ValueError):
                k = json.dumps(str(k), default=lambda o: o.__dict__)

            if hasattr(v, "to_json_dict"):
                v = v.to_json_dict()
                try:
                    v = json.dumps(v, default=lambda o: o.__dict__)
                except (AttributeError, ValueError):
                    raise ValueError(
                        "Object could not be serialised: {}".format(v))
            else:
                try:
                    v = json.dumps(v, default=lambda o: o.__dict__)
                except (AttributeError, ValueError):
                    v = json.dumps(str(v), default=lambda o: o.__dict__)

            data.append("{}: {}".format(k, v))
        data = "{" + ", ".join(data) + "}"
        return "{:.6f};{};{};{};{};{};{}".format(
            self.time, self.level, self.processed_jobs, self.open_jobs,
            json.dumps(self.type),
            json.dumps(self.msg),
            data)

    @classmethod
    def from_entries(cls, parts):
        time = float(parts[0])
        level = int(parts[1])
        processed_jobs = int(parts[2])
        open_jobs = int(parts[3])
        type = parts[4]
        msg = parts[5]
        data = json.loads(parts[6])

        return LoggingEvent(time, level, open_jobs, processed_jobs, msg, type,
                            data)


class EventLogger(Logger):
    """Logger for events which will only log to files and will write the log messages
    without any additional formatting.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, streamhandler=False, **kwargs)

    @property
    def file_formatter(self):
        return logging.Formatter('%(message)s')


def load_events_from_file(in_file):
    events = []
    with open(in_file, 'r') as f:
        reader = csv.reader(f, delimiter=';')
        for row in reader:
            if row:
                events.append(LoggingEvent.from_entries(row))
    return events
