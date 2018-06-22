from batsim.batsim import BatsimScheduler, Batsim, Job

import sys
import os
import logging
from sortedcontainers import SortedSet


class ClemSched(BatsimScheduler):

    def myprint(self,msg):
        print("[{time}] {msg}".format(time=self.bs.time(), msg=msg))


    def __init__(self, options):
        self.options = options

        self.flag1 = True
        self.flag2 = True
        self.idSub = 100

    def onSimulationBegins(self):
        self.bs.logger.setLevel(logging.ERROR)

        self.openJobs = []
        self.nbResources = self.bs.nb_resources
        self.idle = True

        self.bs.wake_me_up_at(4000)

        #prof = {"small": {'type': 'msg_par_hg','cpu': 60e8,'com': 0}}
        #self.bs.submit_profiles("dyn", prof)
        #self.submitStartSmall()

    def onSimulationEnds(self):
        print("")
        self.myprint("Final temperatures: " + str(self.bs.air_temperatures))
        print("")

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
        '''self.myprint("Job_received: " + job.id)
        self.bs.request_processor_temperature_all()

        if (job.id).split('!')[-1] == "1":
            self.bs.start_jobs_continuous([(job, (0,1))])
            self.idle=False

        else:#elif (job.id).split('!')[-1] == "2":
            self.bs.start_jobs_continuous([(job, (0,0))])
            self.idle=False
        '''
        '''else:
            if self.idle == True:
                self.bs.start_jobs_continuous([(job, (2,2))])
                self.idle=False
            else:
                self.bs.reject_jobs([job])'''


    def onJobCompletion(self, job):
        self.myprint("Job_finished: " + job.id)
        self.idle = True
        self.bs.request_processor_temperature_all()

        #if (job.id).split('!')[0] == "dyn":
        #    self.trySubmitSmall()

    def onNoMoreEvents(self):
        #pass
        if self.idle:
            self.scheduleJobs()

    def onRequestedCall(self):
        self.myprint("REQUESTED CALL")
        self.bs.request_processor_temperature_all()
        #self.trySubmitSmall()
        
        self.flag2 = False
        self.flag1 = False
        #self.bs.notify_submission_finished()

    def onAnswerProcessorTemperatureAll(self, proc_temperature_all):
        self.myprint("Proc"+ str(proc_temperature_all))
        self.myprint("Air"+ str(self.bs.air_temperatures) + "\n")

        if not self.flag1 and not self.flag2:
            self.bs.notify_submission_finished()

        
    def trySubmitSmall(self):
        if self.flag1 :
            if self.bs.air_temperatures["2"] < 25:
                self.submitStartSmall()
            else:
                self.flag1 = False

        if self.idSub > 150:
            self.flag1 = False
            #self.flag2 = False
            #self.bs.notify_submission_finished()

    def submitStartSmall(self):
        jid = "dyn!" + str(self.idSub)
        self.bs.submit_job(id=jid, res=1, walltime=-1, profile_name="small")
        self.idSub += 1
        job = Job(jid, 0, -1, 1, "", "", "")
        self.bs.start_jobs_continuous([(job, (2,2))])
        self.myprint("job started " + jid)