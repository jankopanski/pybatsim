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
        self.myprint("Starting Init")

        self.openJobs = []
        self.nbResources = self.bs.nb_res
        self.idle = True
        self.askedAirTemp = False
        self.air_temperature = []
        self.proc_temperature = []

    def scheduleJobs(self):
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
        if self.idle and not self.askedAirTemp:
            self.askedAirTemp = True
            self.myprint("<- Asked for air temperature all")
            self.bs.request_air_temperature_all()


    def onJobCompletion(self, job):
        self.myprint("Job {} completed".format(job.id))
        self.idle = True
        if len(self.openJobs) > 0 and not self.askedAirTemp:
            #self.scheduleJobs()
            self.askedAirTemp = True
            self.myprint("<- Asked for air temperature all")
            self.bs.request_air_temperature_all()

    def onAnswerAirTemperatureAll(self, air_temperature_all):
        self.myprint(" -> Answer air temperature received")
        self.askedAirTemp = False
        self.air_temperature = air_temperature_all
        print("{a}".format(a=self.air_temperature))
        self.scheduleJobs()
