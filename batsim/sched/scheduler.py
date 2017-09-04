"""
    batsim.sched.scheduler
    ~~~~~~~~~~~~~~~~~~~~~~

    This module provides a high-level interface for implementing schedulers for Batsim.
    It contains a basic scheduler used by Pybatsim directly and a high-level scheduler API which will
    interact with the basic scheduler to provide a richer interface.

"""
import logging
import os
from abc import ABCMeta, abstractmethod

from batsim.batsim import BatsimScheduler

from .resource import Resources, Resource
from .job import Job, Jobs
from .reply import ConsumedEnergyReply
from .utils import DictWrapper
from .messages import Message
from .utils import ListView


class BaseBatsimScheduler(BatsimScheduler):
    """The basic Pybatsim scheduler.

    :param scheduler: the high-level scheduler which uses this basic scheduler.

    :param options: the options given to the launcher.
    """

    def __init__(self, scheduler, options):
        self._scheduler = scheduler
        self._options = options

        self._jobmap = {}

    def onAfterBatsimInit(self):
        self._scheduler.debug(
            "decision process is executing after batsim init", type="on_init")
        self._scheduler._batsim = self.bs
        self._scheduler._update_time()
        self._scheduler._on_pre_init()
        self._scheduler.on_init()
        self._scheduler._on_post_init()

    def onSimulationBegins(self):
        self._scheduler.info(
            "Simulation begins",
            type="simulation_begins_received")

    def onSimulationEnds(self):
        self._scheduler._update_time()
        self._scheduler.info(
            "Simulation ends",
            type="simulation_ends_received")
        self._scheduler._on_pre_end()
        self._scheduler.on_end()
        self._scheduler._on_post_end()

    def onNOP(self):
        self._scheduler._update_time()
        self._scheduler.debug(
            "decision process received NOP",
            type="nop_received")
        self._scheduler.on_nop()
        self._scheduler._do_schedule()

    def onJobsKilled(self, jobs):
        self._scheduler._update_time()
        self._scheduler.debug(
            "decision process received jobs kills({jobs})",
            jobs=jobs,
            type="jobs_killed_received2")
        jobobjs = []
        for job in jobs:
            jobobj = self._jobmap[job.id]
            del self._jobmap[job.id]
            jobobjs.append(job)

        self._scheduler.info("The following jobs were killed: ({jobs})",
                             jobs=jobobjs, type="jobs_killed_received")

        for job in jobobjs:
            job._do_complete_job()

        self._scheduler.on_jobs_killed(jobobjs)
        self._scheduler._do_schedule()

    def onJobSubmission(self, job):
        self._scheduler._update_time()
        self._scheduler.debug(
            "decision process received job submission({job})",
            job=job,
            type="job_submission_received2")
        newjob = Job(
            batsim_job=job,
            scheduler=self._scheduler,
            jobs_list=self._scheduler.jobs)
        self._jobmap[job.id] = newjob

        self._scheduler.jobs.add(newjob)

        self._scheduler.info("Received job submission from Batsim ({job})",
                             job=newjob, type="job_submission_received")

        if newjob.is_dynamic_job:
            for job2 in self._scheduler.jobs.dynamic_submission_request:
                if job.id == job2.id:
                    newjob.move_properties_from(job2)
                    self._scheduler.jobs.remove(job2)
                    break
        self._scheduler.on_job_submission(newjob)
        self._scheduler._do_schedule()

    def onJobCompletion(self, job):
        self._scheduler._update_time()
        self._scheduler.debug(
            "decision process received job completion({job})",
            job=job,
            type="job_completion_received2")
        jobobj = self._jobmap[job.id]
        del self._jobmap[job.id]

        self._scheduler.info("Job has completed its execution ({job})",
                             job=jobobj, type="job_completion_received")

        jobobj._do_complete_job()

        self._scheduler.on_job_completion(jobobj)
        self._scheduler._do_schedule()

    def onJobMessage(self, timestamp, job, message):
        self._scheduler._update_time()
        self._scheduler.debug(
            "decision process received from job message({job} => {message})",
            job=job,
            message=message,
            type="job_message_received2")
        jobobj = self._jobmap[job.id]
        self._scheduler.info(
            "Got from job message({job} => {message})",
            job=jobobj,
            message=message,
            type="job_message_received")
        msg = Message(timestamp, message)
        jobobj.messages.append(msg)
        self._scheduler.on_job_message(jobobj, msg)
        self._scheduler._do_schedule()

    def onMachinePStateChanged(self, nodeid, pstate):
        self._scheduler._update_time()
        resource = self._scheduler.resources[nodeid]
        self._scheduler.info(
            "Resource state was updated ({resource}) to {pstate}",
            resource=resource,
            pstate=pstate,
            type="pstate_change_received")

        resource.update_pstate_change(pstate)

        self._scheduler.on_machine_pstate_changed(resource, pstate)
        self._scheduler._do_schedule()

    def onReportEnergyConsumed(self, consumed_energy):
        self._scheduler._update_time()
        self._scheduler.info(
            "Received reply from Batsim (energy_consumed={energy_consumed})",
            energy_consumed=consumed_energy,
            type="reply_energy_received")

        reply = BatsimReply(consumed_energy=consumed_energy)
        self._scheduler.on_report_energy_consumed(reply)
        self._scheduler._do_schedule(reply)


class Scheduler(metaclass=ABCMeta):
    """The high-level scheduler which should be interited from by concrete scheduler
    implementations. All important Batsim functions are either available in the scheduler or used
    by the job/resource objects.

    :param options: the options given to the launcher.

    """

    class Event:
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

    def __init__(self, options={}):
        self._options = options

        self._init_logger()
        self._events = []

        # Use the basic Pybatsim scheduler to wrap the Batsim API
        self._scheduler = BaseBatsimScheduler(self, options)

        self._time = 0

        self._reply = None

        self._sched_delay = float(
            options.get(
                "sched_delay",
                None) or 0.000000000000000000001)

        self._jobs = Jobs()
        self._resources = Resources()

        self.debug("Scheduler initialised", type="scheduler_initialised")

    def _init_logger(self):
        debug = self.options.get("debug", False)
        if isinstance(debug, str):
            debug = debug.lower() in ["y", "yes", "true", "1"]

        self._logger = logging.getLogger(self.__class__.__name__)
        if debug:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '[%(name)s::%(levelname)s] %(message)s')

        # Add the stream handler (stdout)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

        self._event_logger = logging.getLogger(
            self.__class__.__name__ + "Events")
        self._event_logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(message)s')

        export_prefix = self.options.get("export_prefix", "out")

        # Add the persistent event logging handler (not purged between runs)
        handler = logging.FileHandler(
            "{}_events.csv".format(export_prefix))
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        self._event_logger.addHandler(handler)

        # Add the event logging handler for the last run (purged when the next
        # run starts)
        filename_lastschedule = "{}_last_events.csv".format(
            export_prefix)
        try:
            os.remove(filename_lastschedule)
        except OSError:
            pass
        handler = logging.FileHandler(filename_lastschedule)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        self._event_logger.addHandler(handler)

    @property
    def events(self):
        """The events happened in the scheduler."""
        return ListView(self._events)

    @property
    def hpst(self):
        """The hpst (high-performance storage tier) host managed by Batsim."""
        return self._hpst

    @property
    def lcst(self):
        """The lcst (large-capacity storage tier) host managed by Batsim."""
        return self._lcst

    @property
    def pfs(self):
        """The pfs (parallel file system) host managed by Batsim. This is an alias
        to the host of the large-capacity storage tier.
        """
        return self.lcst

    @property
    def options(self):
        """The options given to the launcher."""
        return self._options

    @property
    def resources(self):
        """The searchable collection of resources."""
        return self._resources

    @property
    def jobs(self):
        """The searchable collection of jobs."""
        return self._jobs

    @property
    def reply(self):
        """The last reply from Batsim (or None)."""
        return self._reply

    @property
    def time(self):
        """The current simulation time."""
        return self._time

    @property
    def has_time_sharing(self):
        """Whether or not time sharing is enabled."""
        return self._batsim.time_sharing

    def run_scheduler_at(self, time):
        """Wake the scheduler at the given point in time (of the simulation)."""
        self._batsim.wake_me_up_at(time)

    def request_consumed_energy(self):
        """Request the consumed energy from Batsim."""
        self._batsim.request_consumed_energy()

    def __call__(self):
        """Return the underlying Pybatsim scheduler."""
        return self._scheduler

    def _format_log_msg(self, msg, **kwargs):
        msg = msg.format(**kwargs)
        return "{:.6f} | {}".format(self.time, msg)

    def _format_event_msg(self, level, msg, type="msg", **kwargs):
        msg = msg.format(**kwargs)

        event = Scheduler.Event(self.time, level, msg, type, kwargs)

        self._events.append(event)
        self.on_event(event)

        return str(event)

    def debug(self, msg, **kwargs):
        """Writes a debug message to the logging facility."""
        self._logger.debug(self._format_log_msg(msg, **kwargs))
        self._event_logger.info(self._format_event_msg(1, msg, **kwargs))

    def info(self, msg, **kwargs):
        """Writes a info message to the logging facility."""
        self._logger.info(self._format_log_msg(msg, **kwargs))
        self._event_logger.info(self._format_event_msg(2, msg, **kwargs))

    def warn(self, msg, **kwargs):
        """Writes a warn message to the logging facility."""
        self._logger.warn(self._format_log_msg(msg, **kwargs))
        self._event_logger.info(self._format_event_msg(3, msg, **kwargs))

    def error(self, msg, **kwargs):
        """Writes a error message to the logging facility."""
        self._logger.error(self._format_log_msg(msg, **kwargs))
        self._event_logger.info(self._format_event_msg(4, msg, **kwargs))

    def fatal(self, msg, **kwargs):
        """Writes a fatal message to the logging facility and terminates the scheduler."""
        error_msg = self._format_log_msg(msg, **kwargs)
        self._logger.error(error_msg)
        self._event_logger.info(self._format_event_msg(5, msg, **kwargs))
        raise ValueError("Fatal error: {}".format(error_msg))

    def _on_pre_init(self):
        """The _pre_init method called during the start-up phase of the scheduler.
        If the _pre_init method is overridden the super method should be called with:
        `super()._pre_init()`
        """
        for r in self._batsim.resources:
            self._resources.add(Resource(self,
                                         r["id"],
                                         r["name"],
                                         r["state"],
                                         r["properties"],
                                         self.resources))
        self.info(
            "{num_resources} resources registered",
            num_resources=len(
                self.resources),
            type="resources_registered")

        self._hpst = DictWrapper(self._batsim.hpst)
        self._lcst = DictWrapper(self._batsim.lcst)

    def on_init(self):
        """The init method called during the start-up phase of the scheduler."""
        pass

    def _on_post_init(self):
        """The _post_init method called during the start-up phase of the scheduler.
        If the _post_init method is overridden the super method should be called with:
        `super()._post_init()`
        """
        pass

    def _pre_schedule(self):
        """The _pre_schedule method called during the scheduling phase of the scheduler.
        If the _pre_schedule method is overridden the super method should be called with:
        `super()._pre_schedule()`
        """
        self.debug(
            "Starting scheduling iteration",
            type="scheduling_iteration_started")

        if self.jobs.open:
            self.debug(
                "{num_jobs} jobs open at start of scheduling iteration",
                num_jobs=len(
                    self.jobs.open),
                type="jobs_open_at_start")

    @abstractmethod
    def schedule(self):
        """The schedule method called during the scheduling phase of the scheduler."""
        pass

    def _post_schedule(self):
        """The _post_schedule method called during the scheduling phase of the scheduler.
        If the _post_schedule method is overridden the super method should be called with:
        `super()._post_schedule()`
        """
        if self.jobs.open:
            self.debug(
                "{num_jobs} jobs open at end of scheduling iteration",
                num_jobs=len(
                    self.jobs.open),
                type="jobs_open_at_end")

        self.debug(
            "Ending scheduling iteration",
            type="scheduling_iteration_ended")

    def _update_time(self):
        self._time = self._batsim.time()

    def _do_schedule(self, reply=None):
        """Internal method to execute a scheduling iteration.

        :param reply: the reply set by Batsim (most of the time there is no reply object)
        """
        self._reply = reply
        self._pre_schedule()
        self.schedule()
        self._post_schedule()

        # Fast forward the time after the iteration. The time can be set through
        # a scheduler starting option.
        self._batsim.consume_time(self._sched_delay)

    def _on_pre_end(self):
        """The _pre_end method called during the shut-down phase of the scheduler.
        If the _pre_end method is overridden the super method should be called with:
        `super()._pre_end()`
        """
        if self.jobs.open:
            self.warn(
                "{num_jobs} jobs still in state open at end of simulation",
                num_jobs=len(
                    self.jobs.open),
                type="open_jobs_warning")

    def on_end(self):
        """The end method called during the shut-down phase of the scheduler."""
        pass

    def _on_post_end(self):
        """The _post_end method called during the shut-down phase of the scheduler.
        If the _post_end method is overridden the super method should be called with:
        `super()._post_end()`
        """
        pass

    def on_nop(self):
        """Hook similar to the low-level API."""
        pass

    def on_jobs_killed(self, jobs):
        """Hook similar to the low-level API.

        :param jobs: the killed jobs (higher-level job objects)
        """
        pass

    def on_job_submission(self, job):
        """Hook similar to the low-level API.

        :param job: the submitted job (higher-level job object)
        """
        pass

    def on_job_completion(self, job):
        """Hook similar to the low-level API.

        :param job: the completed job (higher-level job object)
        """
        pass

    def on_job_message(self, job, message):
        """Hook similar to the low-level API.

        :param job: the sending job

        :param message: the sent message
        """
        pass

    def on_machine_pstate_changed(self, resource, pstate):
        """Hook similar to the low-level API.

        :param resource: the changed resource (higher-level job object)

        :param pstate: the new pstate
        """
        pass

    def on_report_energy_consumed(self, consumed_energy):
        """Hook similar to the low-level API.

        :param consumed_energy: the consumed energy (higher-level reply object)
        """
        pass

    def on_event(self, event):
        """Hook called on each event triggered by the scheduler.

        :param event: the triggered event (class: `Scheduler.Event`)
        """
        pass


def as_scheduler(*args, on_init=[], on_end=[], base_classes=[], **kwargs):
    """Decorator to convert a function to a scheduler class.

    The function should accept the scheduler as first argument and optionally
    *args and **kwargs arguments which will be given from additional arguments
    to the call of the decorator.

    :param args: additional arguments passed to the scheduler function (in each iteration)

    :param base_class: the class to use as a base class for the scheduler (must be a subclass of Scheduler)

    :param kwargs: additional arguments passed to the scheduler function (in each iteration)
    """
    base_classes = base_classes.copy()
    base_classes.append(Scheduler)

    def convert_to_scheduler(schedule_function):
        class InheritedScheduler(*base_classes):
            def __init__(self, *init_args, **init_kwargs):
                super().__init__(*init_args, **init_kwargs)

            def _on_pre_init(self):
                super()._on_pre_init()
                for i in on_init:
                    i(self)

            def schedule(self):
                schedule_function(self, *args, **kwargs)

            def _on_pre_end(self):
                super()._on_pre_end()
                for e in on_end:
                    e(self)

        InheritedScheduler.__name__ = schedule_function.__name__

        return InheritedScheduler
    return convert_to_scheduler
