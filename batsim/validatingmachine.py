from batsim.batsim import BatsimScheduler
from sortedcontainers import SortedSet
from procset import ProcSet


class ValidatingMachine(BatsimScheduler):
    """
    This class tries to do a lot of checks to prevent from stupid and invisible errors.

    You should use this when you are developping and testing a scheduler.

    It checks that:
    - not 2 jobs use the same resource as the same time
    - a job is only started once
    - a job is launched after his submit time
    """

    def __init__(self, scheduler):
        super().__init__()
        self.scheduler = scheduler

    def onAfterBatsimInit(self):
        self.scheduler.onAfterBatsimInit()

    def onSimulationBegins(self):
        self.nb_res = self.bs.nb_compute_resources
        self.availableResources = SortedSet(range(self.nb_res))
        self.jobs_waiting = []
        self.previousAllocations = dict()

        # save real job start function
        self.real_start_jobs = self.bs.start_jobs
        self.real_execute_jobs = self.bs.execute_jobs

        # intercept job start
        self.scheduler.bs = self.bs
        self.scheduler.bs.start_jobs = self.start_jobs_valid
        self.scheduler.bs.execute_jobs = self.execute_jobs_valid
        
        self.scheduler.onSimulationBegins()

    def onSimulationEnds(self):
        self.scheduler.onSimulationEnds()

    def onDeadlock(self):
        self.scheduler.onDeadlock()

    def onJobSubmission(self, job):
        self.jobs_waiting.append(job)
        self.scheduler.onJobSubmission(job)

    def onJobCompletion(self, job):
        for res in self.previousAllocations[job.id]:
            self.availableResources.add(res)
        self.previousAllocations.pop(job.id)
        self.scheduler.onJobCompletion(job)

    def onJobMessage(self, timestamp, job, message):
        self.scheduler.onJobMessage(timestamp, job, message)

    def onJobsKilled(self, jobs):
        self.scheduler.onJobsKilled(jobs)

    def onMachinePStateChanged(self, nodeid, pstate):
        self.scheduler.onMachinePStateChanged(nodeid, pstate)

    def onReportEnergyConsumed(self, consumed_energy):
        self.scheduler.onReportEnergyConsumed(consumed_energy)

    def onRequestedCall(self):
        self.scheduler.onRequestedCall()


    def start_jobs_valid(self, jobs, res):
        for j in jobs:
            try:
                self.jobs_waiting.remove(j)
            except KeyError:
                raise ValueError(
                    "Job {} was not waiting (waiting: {})".format(
                        j, [j2.id for j2 in self.jobs_waiting]))
            self.previousAllocations[j.id] = res[j.id]
            for r in res[j.id]:
                try:
                    self.availableResources.remove(r)
                except KeyError:
                    raise ValueError(
                        "Resource {} was not available (available: {})".format(
                            r, list(
                                self.availableResources)))
            j.allocation = ProcSet(*res[j.id])
        self.real_execute_jobs(jobs)

    def execute_jobs_valid(self, jobs, io_jobs=None):
        for j in jobs:
            try:
                self.jobs_waiting.remove(j)
            except KeyError:
                raise ValueError(
                    "Job {} was not waiting (waiting: {})".format(
                        j, [j2.id for j2 in self.jobs_waiting]))
            self.previousAllocations[j.id] = j.allocation
            for r in j.allocation:
                try:
                    self.availableResources.remove(r)
                except KeyError:
                    raise ValueError(
                        "Resource {} was not available (available: {})".format(
                            r, list(
                                self.availableResources)))
        self.real_execute_jobs(jobs, io_jobs)