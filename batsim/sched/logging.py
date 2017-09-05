"""
    batsim.sched.logging
    ~~~~~~~~~~~~~~~~~~~~

    This module provides logging and exporting utilities for data in the scheduler.
"""

import logging
import os


class LoggingEvent:
    """Class for storing data about events triggered by the scheduler.

    :param time: the simulation time when the event occurred

    :param level: the importance level of the event

    :param msg: the actual message of the event

    :param type: the type of the event (`str`)

    :param data: additional data attached to the event (`dict`)
    """

    def __init__(self, time, level, msg, type, data):
        self.time = time
        self.level = level
        self.msg = msg
        self.type = type
        self.data = data

    def __str__(self):
        data = ";".join(
            ["{}={}".format(
                str(k).replace(";", ","),
                str(v).replace(";", ",")) for k, v in self.data.items()])
        return "{:.6f};{};{};{};{}".format(
            self.time, self.level, self.type, self.msg, data)


class Logger:
    """Base logger class which handles various cases needed for the loggers.

    :param obj: either a string with the name of the logger or an object or a type
    (in which case the logger will be based on the type).

    :param logger_suffix: a suffix appended to the generated logger name

    :param debug: whether the output of debug messages should be enabled

    :param streamhandler: whether or not the logger should output to stdout/stderr

    :param to_file: the file to log to if set (will be overwritten each time)

    :param append_to_file: the file to log to if set (new lines will be appended)

    """

    def __init__(
            self,
            obj,
            logger_suffix=None,
            debug=False,
            streamhandler=True,
            to_file=None,
            append_to_file=None):
        if isinstance(obj, type):
            obj = obj.__name__
        elif not isinstance(obj, str):
            obj = obj.__class__.__name__

        if logger_suffix:
            obj = obj + "_" + logger_suffix

        self._logger = logger = logging.getLogger(obj)

        self._debug = debug = debug or str(debug).lower() in [
            "y", "yes", "true", "1"]

        if debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        if streamhandler:
            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            handler.setFormatter(self.formatter)
            logger.addHandler(handler)

        if to_file:
            try:
                os.remove(to_file)
            except OSError:
                pass
            handler = logging.FileHandler(to_file)
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(self.file_formatter)
            logger.addHandler(handler)

        if append_to_file:
            handler = logging.FileHandler(append_to_file)
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(self.file_formatter)
            logger.addHandler(handler)

    @property
    def formatter(self):
        return logging.Formatter('[%(name)s::%(levelname)s] %(message)s')

    @property
    def file_formatter(self):
        return self.formatter

    @property
    def has_debug(self):
        return self._debug

    def debug(self, *args, **kwargs):
        """Writes a debug message to the logger."""
        self._logger.debug(*args, **kwargs)

    def info(self, *args, **kwargs):
        """Writes a info message to the logger."""
        self._logger.info(*args, **kwargs)

    def warn(self, *args, **kwargs):
        """Writes a warn message to the logger."""
        self._logger.warn(*args, **kwargs)

    def error(self, *args, **kwargs):
        """Writes a error message to the logger."""
        self._logger.error(*args, **kwargs)


class EventLogger(Logger):
    """Logger for events which will only log to files and will write the log messages
    without any additional formatting.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, streamhandler=False, **kwargs)

    @property
    def file_formatter(self):
        return logging.Formatter('%(message)s')