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
from .job import Job
from .reply import ConsumedEnergyReply


class BaseBatsimScheduler(BatsimScheduler):
    """The basic Pybatsim scheduler.

    :param scheduler: the high-level scheduler which uses this basic scheduler.

    :param options: the options given to the launcher.
    """

    def __init__(self, scheduler, options):
        self._scheduler = scheduler
        self._options = options

    def onAfterBatsimInit(self):
        self._scheduler.debug(
            "decision process is executing after batsim init")
        self._scheduler._batsim = self.bs
        self._scheduler._pre_init()
        self._scheduler.init()
        self._scheduler._post_init()

    def onSimulationBegins(self):
        self._scheduler.info("Simulation begins")

    def onSimulationEnds(self):
        self._scheduler.info("Simulation ends")
        self._scheduler._pre_end()
        self._scheduler.end()
        self._scheduler._post_end()

    def onNOP(self):
        self._scheduler.debug("decision process received NOP")
        self.do_schedule()

    def onJobsKilled(self, jobs):
        self._scheduler.debug(
            "decision process received jobs kills({})".format(jobs))
        for job in jobs:
            self.complete_job(job)
        self.do_schedule()

    def onJobSubmission(self, job):
        self._scheduler.debug(
            "decision process received job submission({})".format(job))
        newjob = Job(batsim_job=job)
        self._scheduler._open_jobs.append(newjob)
        self._scheduler._new_open_jobs.append(newjob)
        self._scheduler._job_map[job.id] = newjob
        self.do_schedule()

    def onJobCompletion(self, job):
        self._scheduler.debug(
            "decision process received job completion({})".format(job))
        self.complete_job(job)
        self.do_schedule()

    def complete_job(self, job):
        jobobj = self._scheduler._job_map[job.id]
        self._scheduler.info("Remove completed job and free resources: {}"
                             .format(jobobj))
        jobobj.free_all()
        self._scheduler._scheduled_jobs.remove(jobobj)
        self._scheduler._completed_jobs.append(jobobj)
        self._scheduler._new_completed_jobs.append(jobobj)
        del self._scheduler._job_map[job.id]

    def onMachinePStateChanged(self, nodeid, pstate):
        self._scheduler.debug(
            "decision process received machine pstate changed({}, {})".format(
                nodeid, pstate))
        # TODO
        # set resource._state with given nodeid to pstate
        # also add the resource to _new_changed_resources
        self.do_schedule()

    def onReportEnergyConsumed(self, consumed_energy):
        self._scheduler.debug(
            "decision process received energy consumed reply({})".format(
                consumed_energy))
        self.do_schedule(BatsimReply(consumed_energy=consumed_energy))

    def do_schedule(self, reply=None):
        self._scheduler._time = self.bs.time()
        self._scheduler._reply = reply
        self._scheduler._pre_schedule()
        self._scheduler.schedule()
        self._scheduler._post_schedule()

        self.bs.consume_time(self._scheduler._sched_delay)

        self._scheduler._new_open_jobs = []
        self._scheduler._new_completed_jobs = []
        self._scheduler._new_changed_resources = []


class Scheduler(metaclass=ABCMeta):
    """The high-level scheduler which should be interited from by concrete scheduler
    implementations. All important Batsim functions are either available in the scheduler or used
    by the job/resource objects.

    :param options: the options given to the launcher.

    """

    def __init__(self, options):
        self._options = options

        self._init_logger()

        # Use the basic Pybatsim scheduler to wrap the Batsim API
        self._scheduler = BaseBatsimScheduler(self, options)

        self._job_map = {}

        self._time = 0

        self._new_open_jobs = []
        self._new_completed_jobs = []
        self._new_changed_resources = []

        self._open_jobs = []
        self._dyn_jobs = []
        self._completed_jobs = []
        self._rejected_jobs = []
        self._scheduled_jobs = []
        self._reply = None

        self._sched_delay = float(
            options.get(
                "sched_delay",
                None) or 0.00000000000001)

        self._resources = Resources()

        self.debug("Scheduler initialised")

    @property
    def options(self):
        """The options given to the launcher."""
        return self._options

    @property
    def resources(self):
        """The searchable collection of resources."""
        return self._resources

    @property
    def open_jobs(self):
        """The open jobs (yet to be scheduled)."""
        return tuple(self._open_jobs)

    @property
    def completed_jobs(self):
        """The completed jobs (already scheduled)."""
        return tuple(self._completed_jobs)

    @property
    def rejected_jobs(self):
        """The rejected jobs."""
        return tuple(self._rejected_jobs)

    @property
    def scheduled_jobs(self):
        """The currently scheduled jobs."""
        return tuple(self._scheduled_jobs)

    @property
    def reply(self):
        """The last reply from Batsim (or None)."""
        return self._reply

    @property
    def new_open_jobs(self):
        """The jobs which are new in this iteration."""
        return (self._new_open_jobs)

    @property
    def new_completed_jobs(self):
        """The jobs which were completed in this iteration."""
        return tuple(self._new_completed_jobs)

    @property
    def time(self):
        """The current simulation time."""
        return self._time

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

        handler = logging.FileHandler(
            "out_scheduler_{}.log".format(self.__class__.__name__))
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    def run_scheduler_at(self, time):
        """Wake the scheduler at the given point in time (of the simulation)."""
        self._batsim.wake_me_up_at(time)

    def request_consumed_energy(self):
        """Request the consumed energy from Batsim."""
        self._batsim.request_consumed_energy()

    def __call__(self):
        """Return the underlying Pybatsim scheduler."""
        return self._scheduler

    def _pre_init(self):
        """The _pre_init method called during the start-up phase of the scheduler.
        If the _pre_init method is overridden the super method should be called with:
        `super()._pre_init()`
        """
        self._resources = Resources([Resource(self, id)
                                     for id in range(self._batsim.nb_res)])
        self.info("{} resources registered".format(len(self.resources)))

    def init(self):
        """The init method called during the start-up phase of the scheduler."""
        pass

    def _post_init(self):
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
        self.debug("Starting scheduling iteration")

    def _format_msg(self, msg, *args, **kwargs):
        msg = msg.format(*args, **kwargs)
        return "{:.6f} | {}".format(self.time, msg)

    def debug(self, msg, *args, **kwargs):
        """Writes a debug message to the logging facility."""
        self._logger.debug(self._format_msg(msg, *args, **kwargs))

    def info(self, msg, *args, **kwargs):
        """Writes a info message to the logging facility."""
        self._logger.info(self._format_msg(msg, *args, **kwargs))

    def warn(self, msg, *args, **kwargs):
        """Writes a warn message to the logging facility."""
        self._logger.warn(self._format_msg(msg, *args, **kwargs))

    def error(self, msg, *args, **kwargs):
        """Writes a error message to the logging facility."""
        self._logger.error(self._format_msg(msg, *args, **kwargs))

    @abstractmethod
    def schedule(self):
        """The schedule method called during the scheduling phase of the scheduler."""
        pass

    def _post_schedule(self):
        """The _post_schedule method called during the scheduling phase of the scheduler.
        If the _post_schedule method is overridden the super method should be called with:
        `super()._post_schedule()`
        """
        for j in self._dyn_jobs[:]:
            if j.dynamically_submitted:
                j._do_dyn_submit(self)

        for j in self._open_jobs[:]:
            if j.scheduled:
                j._do_execute(self)
            elif j.rejected:
                j._do_reject(self)

        for j in self._scheduled_jobs[:]:
            if j.killed:
                j._do_kill(self)

        for r in self._resources:
            if r._state != r._new_state:
                r._do_change_state(self)

        if self._open_jobs:
            self.debug(
                "{} jobs open at end of scheduling iteration", len(
                    self._open_jobs))

        self.debug("Ending scheduling iteration")

    def _pre_end(self):
        """The _pre_end method called during the shut-down phase of the scheduler.
        If the _pre_end method is overridden the super method should be called with:
        `super()._pre_end()`
        """
        if self._open_jobs:
            self.warn(
                "{} jobs still in state open at end of simulation", len(
                    self._open_jobs))

        if self._scheduled_jobs:
            self.warn(
                "{} jobs still in state scheduled at end of simulation", len(
                    self._scheduled_jobs))

    def end(self):
        """The end method called during the shut-down phase of the scheduler."""
        pass

    def _post_end(self):
        """The _post_end method called during the shut-down phase of the scheduler.
        If the _post_end method is overridden the super method should be called with:
        `super()._post_end()`
        """
        pass
