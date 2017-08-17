import logging
from abc import ABCMeta, abstractmethod

from batsim.batsim import BatsimScheduler

from .resource import Resources, Resource
from .job import Job
from .reply import ConsumedEnergyReply

class BaseBatsimScheduler(BatsimScheduler):

    def __init__(self, scheduler, options):
        self._scheduler = scheduler
        self._options = options

    def onAfterBatsimInit(self):
        self._scheduler.debug("decision process is executing after batsim init")
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
        self._scheduler.debug("decision process received jobs kills({})".format(jobs))
        for job in jobs:
            self.complete_job(job)
        self.do_schedule()

    def onJobSubmission(self, job):
        self._scheduler.debug("decision process received job submission({})".format(job))
        newjob = Job(batsim_job=job)
        self._scheduler._open_jobs.append(newjob)
        self._scheduler._new_open_jobs.append(newjob)
        self._scheduler._job_map[job.id] = newjob
        self.do_schedule()

    def onJobCompletion(self, job):
        self._scheduler.debug("decision process received job completion({})".format(job))
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
        self._scheduler.debug("decision process received machine pstate changed({}, {})".format(nodeid, pstate))
        # TODO
        #self._scheduler._changed_machines.append((nodeid, pstate))
        self.do_schedule()

    def onReportEnergyConsumed(self, consumed_energy):
        self._scheduler.debug("decision process received energy consumed reply({})".format(consumed_energy))
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



class Scheduler(metaclass=ABCMeta):

    def __init__(self, options):
        self._options = options

        self._init_logger()

        self._scheduler = BaseBatsimScheduler(self, options)

        self._job_map = {}

        self._time = 0
        self._new_open_jobs = []
        self._new_completed_jobs = []

        self._open_jobs = []
        self._completed_jobs = []
        self._rejected_jobs = []
        self._scheduled_jobs = []
        self._reply = None
        self._sched_delay = float(options.get("sched_delay", None) or 0.00000000000001)

        self._resources = Resources()

        self.debug("Scheduler initialised")

    @property
    def resources(self):
        return self._resources

    @property
    def open_jobs(self):
        return tuple(self._open_jobs)

    @property
    def completed_jobs(self):
        return tuple(self._completed_jobs)

    @property
    def rejected_jobs(self):
        return tuple(self._rejected_jobs)

    @property
    def scheduled_jobs(self):
        return tuple(self._scheduled_jobs)

    @property
    def reply(self):
        return self._reply

    @property
    def new_open_jobs(self):
        return (self._new_open_jobs)

    @property
    def new_completed_jobs(self):
        return tuple(self._new_completed_jobs)

    @property
    def time(self):
        return self._time

    def _init_logger(self):
        debug = self._options.get("debug", False)
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

    def __call__(self):
        return self._scheduler

    def _pre_init(self):
        self._resources = Resources([Resource(self, id) for id in range(self._batsim.nb_res)])
        self.info("{} resources registered".format(len(self.resources)))

    def init(self):
        pass

    def _post_init(self):
        pass

    def _pre_schedule(self):
        self.debug("Starting scheduling iteration")

    def _format_msg(self, msg, *args, **kwargs):
        msg = msg.format(*args, **kwargs)
        return "{:.6f} | {}".format(self.time, msg)

    def debug(self, msg, *args, **kwargs):
        self._logger.debug(self._format_msg(msg, *args, **kwargs))

    def info(self, msg, *args, **kwargs):
        self._logger.info(self._format_msg(msg, *args, **kwargs))

    def warn(self, msg, *args, **kwargs):
        self._logger.warn(self._format_msg(msg, *args, **kwargs))

    def error(self, msg, *args, **kwargs):
        self._logger.error(self._format_msg(msg, *args, **kwargs))

    @abstractmethod
    def schedule(self):
        pass

    def _post_schedule(self):
        for j in self._open_jobs[:]:
            if j.scheduled:
                j.submit(self)
            elif j.rejected:
                j._do_reject(self)
        if self._open_jobs:
            self.debug(
                    "{} jobs open at end of scheduling iteration", len(self._open_jobs))
        self.debug("Ending scheduling iteration")

    def _pre_end(self):
        if self._open_jobs:
            self.warn("{} jobs still in state open at end of simulation", len(self._open_jobs))

        if self._scheduled_jobs:
            self.warn("{} jobs still in state scheduled at end of simulation", len(self._scheduled_jobs))

    def end(self):
        pass

    def _post_end(self):
        pass
