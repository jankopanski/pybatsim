"""
    batsim.sched.scheduler
    ~~~~~~~~~~~~~~~~~~~~~~

    This module provides a high-level interface for implementing schedulers for Batsim.
    It contains a basic scheduler used by Pybatsim directly and a high-level scheduler API which will
    interact with the basic scheduler to provide a richer interface.

"""
from abc import ABCMeta, abstractmethod

from batsim.batsim import BatsimScheduler, Batsim
from batsim.network import NetworkHandler
from batsim.tools.launcher import launch_scheduler_main

from .resource import Resources, ComputeResource
from .job import Job, Jobs
from .reply import ConsumedEnergyReply
from .utils import DictWrapper
from .messages import Message
from .utils import ListView
from .logging import Logger
from .events import LoggingEvent, EventLogger, EventList
from .workloads import WorkloadDescription


class BaseBatsimScheduler(BatsimScheduler):
    """The basic Pybatsim scheduler.

    :param scheduler: the high-level scheduler which uses this basic scheduler.

    :param options: the options given to the launcher.
    """

    def __init__(self, scheduler, options):
        super().__init__(options)

        self._scheduler = scheduler

        self._jobmap = {}
        self._next_job_number = 0

    def onSimulationBegins(self):
        self._scheduler.info(
            "Simulation begins",
            type="simulation_begins_received")
        self._scheduler._batsim = self.bs
        self._scheduler._update_time()
        self._scheduler._on_pre_init()
        self._scheduler.on_init()
        self._scheduler._on_post_init()

    def onSimulationEnds(self):
        self._scheduler._update_time()
        self._scheduler.info(
            "Simulation ends",
            type="simulation_ends_received")
        self._scheduler._on_pre_end()
        self._scheduler.on_end()
        self._scheduler._on_post_end()

    def onDeadlock(self):
        self._scheduler.debug(
            "batsim has reached a deadlock or is not responding",
            type="deadlock")
        self._scheduler.on_deadlock()

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
            jobobj.progress = job.progress
            jobobjs.append(jobobj)

        self._scheduler.info("The following jobs were killed: ({jobs})",
                             jobs=jobobjs, type="jobs_killed_received")

        jobobj._do_complete_job()

        self._scheduler.on_jobs_killed(jobobjs)
        self._scheduler._do_schedule()

    def onJobSubmission(self, job):
        self._scheduler._update_time()
        self._scheduler.debug(
            "decision process received job submission({job})",
            job=job,
            type="job_submission_received2")
        newjob = Job(
            number=self._next_job_number,
            batsim_job=job,
            scheduler=self._scheduler,
            jobs_list=self._scheduler.jobs)
        self._jobmap[job.id] = newjob
        self._next_job_number += 1

        self._scheduler.jobs.add(newjob)

        job_id_split = job.id.split(Batsim.WORKLOAD_JOB_SEPARATOR)
        workload_name = job_id_split[0]
        job_id = int(job_id_split[1])

        workload = None
        try:
            # Will succeed if job is dynamic job
            workload = self._scheduler._workload_map[workload_name]
        except KeyError:
            pass
        if workload:
            job_description = workload[job_id]
            job_description.job = newjob
            newjob._workload_description = workload

        self._scheduler.info(
            "Received job submission from Batsim (job={job}, open_jobs_in_queue={open_jobs_in_queue})",
            job=newjob,
            open_jobs_in_queue=len(
                self._scheduler.jobs.open),
            type="job_submission_received")

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

    def onAddResources(self, resources):
        self._scheduler._update_time()
        self._scheduler.info(
            "Received add Resources message: {resources}",
            resources=resources,
            type="add_resources_received")
        self._scheduler.on_add_resources(resources)

    def onRemoveResources(self, resources):
        self._scheduler.info(
            "Received remove Resources message: {resources}",
            resources=resources,
            type="remove_resources_received")
        self._scheduler.on_remove_resources(resources)


class Scheduler(metaclass=ABCMeta):
    """The high-level scheduler which should be interited from by concrete scheduler
    implementations. All important Batsim functions are either available in the scheduler or used
    by the job/resource objects.

    :param options: the options given to the launcher.

    """

    @classmethod
    def launch_main(cls, **kwargs):
        """Initialise this scheduler class and run it as if it were started with the launcher."""
        launch_scheduler_main(cls, **kwargs)

    def __init__(self, options={}):
        self._options = options
        debug = self.options.get("debug", False)
        export_prefix = self.options.get("export-prefix", "out")
        self._log_debug_events = self.options.get("log-debug-events", False)

        # Create the logger
        self._logger = Logger(self, debug=debug)

        self._last_published_event = None
        self._event_logger = None
        self._event_logger = EventLogger(
            self, "Events", debug=debug,
            to_file="{}_events.csv".format(export_prefix))
        self._event_logger.info(LoggingEvent.get_csv_header())

        self._events = EventList()

        # Use the basic Pybatsim scheduler to wrap the Batsim API
        self._scheduler = BaseBatsimScheduler(self, options)

        self._time = 0

        self._reply = None

        self._sched_delay = float(
            options.get(
                "sched-delay",
                None) or 0.000000000000000000001)

        self._jobs = Jobs()
        self._resources = Resources()

        self._find_resource_handler = []

        self._workload_map = {}
        self._dynamic_workload = WorkloadDescription(name="DYNAMIC_WORKLOAD")

        self.debug("Scheduler initialised", type="scheduler_initialised")

    @property
    def events(self):
        """The events happened in the scheduler."""
        return self._events

    @property
    def dynamic_workload(self):
        """The workload of dynamic job submissions of this scheduler."""
        return self._dynamic_workload

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

    @property
    def get_find_resource_handlers(self):
        """The functions to find resource requirements for jobs."""
        return self._find_resource_handler

    def register_find_resource_handler(self, handler):
        """Adds a resource handler for searching resource requirements for jobs.

        :param handler: a function which should return an iterable
                        (or generator) containing `ResourceRequirement`
                        objects. The function should determine
                        absolutely necessary resource requirements
                        needed by this job. For example, when all jobs
                        should always allocate a specific external
                        special resource like allocating I/O nodes
                        not managed by Batsim.
                        Signature: scheduler, job
        """
        self._find_resource_handler.append(handler)

    def unregister_find_resource_handler(self, handler):
        """Removes a resource handler.

        :param handler: the function to be removed
        """
        self._find_resource_handler.remove(handler)

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

        try:
            open_jobs = self._batsim.nb_jobs_received
            processed_jobs = (self._batsim.nb_jobs_completed +
                              self._batsim.nb_jobs_failed +
                              self._batsim.nb_jobs_rejected +
                              self._batsim.nb_jobs_timeout +
                              self._batsim.nb_jobs_killed +
                              len(self._batsim.jobs_manually_changed))
        except AttributeError:
            # Batsim is not initialised
            open_jobs = 0
            processed_jobs = 0

        event = LoggingEvent(self.time, level, open_jobs, processed_jobs,
                             msg, type, kwargs)

        self._events.add(event)

        event_str = str(event)

        try:
            do_publish = True

            if self._last_published_event is not None:
                if (self._last_published_event.time >= event.time and
                    self._last_published_event.open_jobs == event.open_jobs and
                        self._last_published_event.processed_jobs == event.processed_jobs):
                    do_publish = False

            if do_publish:
                self._batsim.publish_event(event_str)
                self._last_published_event = event
        except AttributeError:
            # Batsim is not initialised
            pass

        self.on_event(event)

        return event.to_csv_line()

    def debug(self, msg, **kwargs):
        """Writes a debug message to the logging facility."""
        if self._log_debug_events:
            self._logger.debug(self._format_log_msg(msg, **kwargs))
            event = self._format_event_msg(1, msg, **kwargs)
            self._event_logger.info(event)

    def info(self, msg, **kwargs):
        """Writes a info message to the logging facility."""
        self._logger.info(self._format_log_msg(msg, **kwargs))
        event = self._format_event_msg(2, msg, **kwargs)
        self._event_logger.info(event)

    def warn(self, msg, **kwargs):
        """Writes a warn message to the logging facility."""
        self._logger.warn(self._format_log_msg(msg, **kwargs))
        event = self._format_event_msg(3, msg, **kwargs)
        self._event_logger.info(event)

    def error(self, msg, **kwargs):
        """Writes a error message to the logging facility."""
        self._logger.error(self._format_log_msg(msg, **kwargs))
        event = self._format_event_msg(4, msg, **kwargs)
        self._event_logger.info(event)

    def fatal(self, msg, **kwargs):
        """Writes a fatal message to the logging facility and terminates the scheduler."""
        error_msg = self._format_log_msg(msg, **kwargs)
        self._logger.error(error_msg)
        event = self._format_event_msg(5, msg, **kwargs)
        self._event_logger.info(event)
        raise ValueError("Fatal error: {}".format(error_msg))

    def _on_pre_init(self):
        """The _pre_init method called during the start-up phase of the scheduler.
        If the _pre_init method is overridden the super method should be called with:
        `super()._pre_init()`
        """
        for r in self._batsim.resources.values():
            self._resources.add(ComputeResource(self,
                                                id=r["id"],
                                                name=r["name"],
                                                resources_list=self.resources,
                                                state=r["state"],
                                                properties=r["properties"]))
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

    def on_deadlock(self):
        raise ValueError("Batsim has reached a deadlock")

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

    def on_add_resources(self, resources):
        """Hook similar to the low-level API.

        :param resources: a procset of resources
        """
        pass

    def on_remove_resources(self, resources):
        """Hook similar to the low-level API.

        :param resources: a procset of resources
        """
        pass

    def on_event(self, event):
        """Hook called on each event triggered by the scheduler.

        :param event: the triggered event (class: `LoggingEvent`)
        """
        pass

    def submit_dynamic_job(self, *args, **kwargs):
        job = self._dynamic_workload.new_job(*args, **kwargs)
        self._dynamic_workload.prepare()
        job.submit(self)


def as_scheduler(*args, on_init=[], on_end=[], base_classes=[], **kwargs):
    """Decorator to convert a function to a scheduler class.

    The function should accept the scheduler as first argument and optionally
    `*args` and `**kwargs` arguments which will be given from additional arguments
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
