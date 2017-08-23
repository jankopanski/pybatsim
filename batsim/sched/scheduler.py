"""
    batsim.sched.scheduler
    ~~~~~~~~~~~~~~~~~~~~~~

    This module provides a high-level interface for implementing schedulers for Batsim.
    It contains a basic scheduler used by Pybatsim directly and a high-level scheduler API which will
    interact with the basic scheduler to provide a richer interface.

"""
import logging
from abc import ABCMeta, abstractmethod

from batsim.batsim import BatsimScheduler

from .resource import Resources, Resource
from .job import Job, Jobs
from .reply import ConsumedEnergyReply


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
        self._scheduler._on_pre_init()
        self._scheduler.on_init()
        self._scheduler._on_post_init()

    def onSimulationBegins(self):
        self._scheduler.info(
            "Simulation begins",
            type="simulation_begins_received")

    def onSimulationEnds(self):
        self._scheduler.info(
            "Simulation ends",
            type="simulation_ends_received")
        self._scheduler._on_pre_end()
        self._scheduler.on_end()
        self._scheduler._on_post_end()

    def onNOP(self):
        self._scheduler.debug(
            "decision process received NOP",
            type="nop_received")
        self._scheduler.on_nop()
        self._scheduler._do_schedule()

    def onJobsKilled(self, jobs):
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
            job._do_complete_job(self._scheduler)

        self._scheduler.on_jobs_killed(jobobjs)
        self._scheduler._do_schedule()

    def onJobSubmission(self, job):
        self._scheduler.debug(
            "decision process received job submission({job})",
            job=job,
            type="job_submission_received2")
        newjob = Job(batsim_job=job, scheduler=self._scheduler)
        self._jobmap[job.id] = newjob

        self._scheduler.jobs.add(newjob)

        self._scheduler.info("Received job submission from Batsim ({job})",
                             job=newjob, type="job_submission_received")

        if newjob.is_user_job:
            for job2 in self._scheduler.jobs.dynamically_submitted:
                if job.id == job2.id:
                    newjob.move_properties_from(job2)
                    self._scheduler.jobs.remove(job2)
                    break
        self._scheduler.on_job_submission(newjob)
        self._scheduler._do_schedule()

    def onJobCompletion(self, job):
        self._scheduler.debug(
            "decision process received job completion({job})",
            job=job,
            type="job_completion_received2")
        jobobj = self._jobmap[job.id]
        del self._jobmap[job.id]

        self._scheduler.info("Job has completed its execution ({job})",
                             job=jobobj, type="job_completion_received")

        jobobj._do_complete_job(self._scheduler)

        self._scheduler.on_job_completion(jobobj)
        self._scheduler._do_schedule()

    def onMachinePStateChanged(self, nodeid, pstate):
        resource = self._scheduler.resources[nodeid]
        self._scheduler.info(
            "Resource state was updated ({resource}) to {pstate}",
            resource=resource,
            pstate=pstate,
            type="pstate_change_received")

        resource.update_pstate_change(pstate)

        self._scheduler.on_machine_pstate_changed(nodeid, pstate)
        self._scheduler._do_schedule()

    def onReportEnergyConsumed(self, consumed_energy):
        self._scheduler.info(
            "Received reply from Batsim (energy_consumed={energy_consumed})",
            energy_consumed=consumed_energy,
            type="reply_energy_received")

        self._scheduler.on_report_energy_consumed(consumed_energy)
        self._scheduler._do_schedule(
            BatsimReply(consumed_energy=consumed_energy))


class Scheduler(metaclass=ABCMeta):
    """The high-level scheduler which should be interited from by concrete scheduler
    implementations. All important Batsim functions are either available in the scheduler or used
    by the job/resource objects.

    :param options: the options given to the launcher.

    """

    class Event:

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

    def __init__(self, options):
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
                None) or 0.00000000000001)

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

        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

        self._event_logger = logging.getLogger(
            self.__class__.__name__ + "Events")
        self._event_logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(message)s')

        handler = logging.FileHandler(
            "out_scheduler_events_{}.csv".format(self.__class__.__name__))
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        self._event_logger.addHandler(handler)

    @property
    def events(self):
        return tuple(self._events)

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

    def run_scheduler_at(self, time):
        """Wake the scheduler at the given point in time (of the simulation)."""
        self._batsim.wake_me_up_at(time)

    def request_consumed_energy(self):
        """Request the consumed energy from Batsim."""
        self._batsim.request_consumed_energy()

    def __call__(self):
        """Return the underlying Pybatsim scheduler."""
        return self._scheduler

    @property
    def has_time_sharing(self):
        return self._batsim.time_sharing

    def _on_pre_init(self):
        """The _pre_init method called during the start-up phase of the scheduler.
        If the _pre_init method is overridden the super method should be called with:
        `super()._pre_init()`
        """
        resources = []
        for r in self._batsim.resources:
            resources.append(Resource(self,
                                      r["id"],
                                      r["name"],
                                      r["state"],
                                      r["properties"]))
        self._resources = Resources(resources)
        self.info(
            "{num_resources} resources registered",
            num_resources=len(
                self.resources),
            type="resources_registered")

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

    def _format_log_msg(self, msg, **kwargs):
        msg = msg.format(**kwargs)
        return "{:.6f} | {}".format(self.time, msg)

    def _format_event_msg(self, level, msg, type="msg", **kwargs):
        msg = msg.format(**kwargs)

        event = Scheduler.Event(self.time, level, msg, type, kwargs)

        self._events.append(event)

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

    @abstractmethod
    def schedule(self):
        """The schedule method called during the scheduling phase of the scheduler."""
        pass

    def _post_schedule(self):
        """The _post_schedule method called during the scheduling phase of the scheduler.
        If the _post_schedule method is overridden the super method should be called with:
        `super()._post_schedule()`
        """
        for r in self._resources:
            if r._pstate_update_request_necessary:
                r._do_change_state(self)

        for j in self.jobs.filter(marked_for_dynamic_submission=True):
            j._do_dyn_submit(self)

        for j in self.jobs.marked_for_rejection:
            j._do_reject(self)

        for j in self.jobs.marked_for_killing:
            j._do_kill(self)

        for j in self.jobs.marked_for_scheduling:
            j._do_execute(self)

        if self.jobs.open:
            self.debug(
                "{num_jobs} jobs open at end of scheduling iteration",
                num_jobs=len(
                    self.jobs.open),
                type="jobs_open_at_end")

        self.debug(
            "Ending scheduling iteration",
            type="scheduling_iteration_ended")

    def _do_schedule(self, reply=None):
        self._time = self._batsim.time()
        self._reply = reply
        self._pre_schedule()
        self.schedule()
        self._post_schedule()

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
        pass

    def on_jobs_killed(self, jobs):
        pass

    def on_job_submission(self, job):
        pass

    def on_job_completion(self, job):
        pass

    def on_machine_pstate_changed(self, nodeid, pstate):
        pass

    def on_report_energy_consumed(self, consumed_energy):
        pass


def as_scheduler(*args, base_class=Scheduler, **kwargs):
    def convert_to_scheduler(schedule_function):
        class InheritedScheduler(base_class):
            def schedule(self):
                schedule_function(self, *args, **kwargs)
        InheritedScheduler.__name__ = schedule_function.__name__

        return InheritedScheduler
    return convert_to_scheduler
