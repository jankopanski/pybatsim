from batsim.batsim import BatsimScheduler, Batsim

import sys
import os
from sortedcontainers import SortedSet


class ClemSched(BatsimScheduler):

    def myprint(self,msg):
        print("[CLEMSCHED {time}] {msg}".format(time=self.bs.time(), msg=msg))


    def __init__(self, options):
        self.options = options

    def onAfterBatsimInit(self):
        self.myprint("Starting Init")

        self.openJobs = []
        self.nbResources = self.bs.nb_res
        self.idle = True


    def scheduleJobs(self):
        if len(self.openJobs) > 0:
            job = self.openJobs.pop(0)
            if job.requested_resources <= self.nbResources:
                toSchedule = [(job, (0, job.requested_resources-1))]
                self.idle = False
                self.bs.start_jobs_continuous(toSchedule)
                self.myprint("<- Scheduled job {}".format(job.id))
            else:
                self.bs.reject_jobs([job])


    def onJobSubmission(self, job):
        self.myprint(" -> Received job {}".format(job))

        self.openJobs.append(job)
        if self.idle:
            self.scheduleJobs()

    def onJobCompletion(self, job):
        self.myprint("Job {} completed".format(job.id))
        self.idle = True
        self.scheduleJobs()
