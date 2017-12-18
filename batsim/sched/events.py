"""
    batsim.sched.events
    ~~~~~~~~~~~~~~~~~~~

    This module provides handling of scheduling events.
"""

import logging
import json
import csv
import io

from .logging import Logger
from .utils import ObserveList, filter_list


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

    def __str__(self):
        return "[{:.6f}] {}/{} <{}> ({})".format(
            self.time, self.processed_jobs, self.open_jobs,
            self.type, self.msg)

    @classmethod
    def get_csv_header(self):
        output = io.StringIO()
        csvdata = ["time", "level", "processed_jobs", "open_jobs",
                   "type", "message", "data"]
        writer = csv.writer(
            output,
            quoting=csv.QUOTE_NONNUMERIC,
            delimiter=';')
        writer.writerow(csvdata)
        return output.getvalue().strip()

    def to_csv_line(self):
        def conv_obj(o):
            try:
                return o.__dict__
            except (AttributeError, ValueError):
                return str(o)

        data = {}
        for k, v in self.data.items():
            k = str(k)

            if hasattr(v, "to_json_dict"):
                v = v.to_json_dict()
            elif hasattr(v, "__iter__") and not isinstance(v, str):
                new_v = []
                for e in v:
                    if hasattr(e, "to_json_dict"):
                        e = e.to_json_dict()
                    new_v.append(e)
                v = new_v
            data[k] = v
        try:
            data = json.dumps(data, default=lambda o: conv_obj(o))
        except Exception as e:
            raise ValueError(
                "Error while dumping json data: {}"
                .format(data))

        output = io.StringIO()
        csvdata = [self.time, self.level, self.processed_jobs, self.open_jobs,
                   self.type, self.msg, data]
        writer = csv.writer(
            output,
            quoting=csv.QUOTE_NONNUMERIC,
            delimiter=';')
        writer.writerow(csvdata)
        return output.getvalue().strip()

    @classmethod
    def from_entries(cls, parts):
        time = float(parts[0])
        level = int(parts[1])
        processed_jobs = int(parts[2])
        open_jobs = int(parts[3])
        type = parts[4]
        msg = parts[5]
        try:
            data = json.loads(parts[6])
        except Exception:
            raise ValueError(
                "Error while parsing data entry in line: {}"
                .format(parts))

        return LoggingEvent(time, level, open_jobs, processed_jobs, msg, type,
                            data)


class EventList(ObserveList):
    def filter(
            self,
            *args,
            time_after=None,
            time_at=None,
            time_before=None,
            level=None,
            types=[],
            **kwargs):
        """Filter the event list to search for specific events.

        :param time_after: Search for events after a specified time.

        :param time_at: Search for events at a specified time.

        :param time_before: Search for events before a specified time.

        :param level: Search for events with a given logging level.

        :param types: Search for events with one of the given event types.
        """

        no_filters = False
        if time_after is None and time_at is None and time_before is None and \
                level is None and not types:
            no_filters = True

        # Filter events
        def filter_events(events, **kwargs):
            if no_filters:
                yield from events
            else:
                for e in events:
                    if time_after is not None:
                        if e.time > time_after:
                            yield e
                            continue
                    if time_before is not None:
                        if e.time < time_before:
                            yield e
                            continue
                    if time_at is not None:
                        if e.time == time_at:
                            yield e
                            continue
                    if level is not None:
                        if e.level == level:
                            yield e
                            continue
                    if types:
                        if e.type in types:
                            yield e
                            continue

        return self.create(
            filter_list(
                self._data,
                [filter_events],
                *args,
                **kwargs))


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
    events = EventList()
    reader = csv.reader(in_file, quoting=csv.QUOTE_NONNUMERIC, delimiter=';')

    # Skip header
    next(reader)

    for row in reader:
        if row:
            events.add(LoggingEvent.from_entries(row))
    return events
