from batsim.batsim import BatsimScheduler, Batsim

import sys
import os
from sortedcontainers import SortedSet


class ClemSched(BatsimScheduler):

    def myprint(self,msg):
        print("[CLEMSCHED {time}] {msg}".format(time=self.bs.time(), msg=msg))


    def __init__(self, options):
        self.options = options

        self.flag1 = True
        self.flag2 = True
        self.idSub = 100

    def onSimulationBegins(self):

        self.openJobs = []
        self.nbResources = self.bs.nb_res
        self.idle = True


        prof = {"small": {'type': 'msg_par_hg','cpu': 60e8,'com': 0}}
        self.bs.submit_profiles("dyn", prof)
        self.bs.request_air_temperature_all()

    def scheduleJobs(self):
        if len(self.openJobs) > 0:
            job = self.openJobs.pop(0)
            if job.requested_resources <= self.nbResources:
                toSchedule = [(job, (0, job.requested_resources-1))]
                self.idle = False
                self.bs.start_jobs_continuous(toSchedule)
            else:
                self.bs.reject_jobs([job])



    def onJobSubmission(self, job):
        self.openJobs.append(job)

    def onJobCompletion(self, job):
        self.idle = True

        print("\nJob_finished:", job.id,self.bs.air_temperatures, "\n")

        self.trySubmitSmall()

    def onNoMoreEvents(self):
        if self.idle:
            self.scheduleJobs()

    def onRequestedCall(self):
        self.trySubmitSmall()

    def onAnswerAirTemperatureAll(self, air_temperature_all):
        print("AAA\n\n", air_temperature_all)
        print("\n\n")

        
    def trySubmitSmall(self):
        if self.bs.air_temperatures[1] < 22:
            if self.flag1:
                self.bs.submit_job(id=("dyn!" + str(self.idSub)), res=2, walltime=-1, profile_name="small")
                self.idSub += 1
            else:
                self.bs.wake_me_up_at(self.bs.time()+60.0)
        else:
            self.flag1 = False

        if self.idSub > 133:
            self.flag1 = False
            self.flag2 = False
            #self.bs.notify_submission_finished()




    #def submitSingleJob(self):
