from batsim.batsim import BatsimScheduler, Batsim

import sys
import os
from sortedcontainers import SortedSet


class QarnotSched(BatsimScheduler):

    def myprint(self,msg):
        print("[QarnotSched {time}] {msg}".format(time=self.bs.time(), msg=msg))


    def __init__(self, options):
        self.options = options

    def onAfterBatsimInit(self):
        self.openJobs = []
        self.nbResources = self.bs.nb_resources
        self.idle = True
        
    def scheduleJobs(self):
        self.myprint("\t\t\t\t{}".format(self.bs.air_temperatures))
        job = self.openJobs.pop(0)
        if job.requested_resources <= self.nbResources:
            toSchedule = [(job, (0, job.requested_resources-1))]
            self.idle = False
            self.bs.start_jobs_continuous(toSchedule)
        else:
            self.bs.reject_jobs([job])


    def onJobSubmission(self, job):

        self.openJobs.append(job)
        if self.idle:
            self.scheduleJobs()


    def onJobCompletion(self, job):
        self.idle = True
        if len(self.openJobs) > 0:
            self.scheduleJobs()
