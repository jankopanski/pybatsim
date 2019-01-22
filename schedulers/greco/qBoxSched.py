from batsim.batsim import BatsimScheduler, Batsim, Job

from collections import defaultdict

import sys
import os
import logging
from procset import ProcSet

''' This is the QBox scheduler
'''
class QBoxSched(BatsimScheduler):
    def __init__(self, options):
        super().__init__(options)

        self.qbox_id = options["qbox_id"]
        tmp_list = options["resource_ids"]
        self.list_qrads = [x for (x,_) in tmp_list]
        self.nb_qrads = len(self.list_qrads)
        self.qrads_properties = {}

        for (index, properties) in tmp_list:
            watts = (properties["watt_per_state"]).split(', ')
            properties["nb_pstates"] = len(watts)
            properties["watt_per_state"] = [float((x.split(':'))[-1]) for x in watts]
            self.qrads_properties[index] = properties


        print("-----Hello, I'm QBox scheduler number {} and there are {} QRads under my watch!".format(self.qbox_id, self.nb_qrads))

        #TODO do something with the temperature schema.
        # Compute the amount of work needed (at least for the next 6 hours or something)
        # Send it to the QNode
        self.requiredTemp = 25.0 # THIS IS AN UGLY HARDCODING è_é TODO REMOVE IT
        self.temperatureDiffs = {} # Stores the temperature difference b/w requiredTemp and actual airTemp for each resource.

        self.nextDynId = 0

        #self.jobQueue = []                                          # stores the jobs waiting to be scheduled (or rejected)
        self.processingJobs = {}                                    # maps the job id that is running to the resource id
        self.cpuBurn = {}                                           # maps the resource id to the job id
        self.idleResource = {x:2 for x in self.list_qrads}          # whether the resource is running a job (0), a CPU burn (1) or idle (2)
        self.resourcePstate = {x:0 for x in self.list_qrads}        # maps the resource id to its current Pstate

        # keys are the target states, values are ProcSets of resources to which to change the state
        self.stateChanges = defaultdict(ProcSet)

        self.flag = True

    def onBeforeEvents(self):
        self.updateHeatingReq()
        '''print("\\\\\\", self.qbox_id)
        print("cpuBurn", self.cpuBurn)
        print("processingJobs", self.processingJobs)
        print("idleResource", self.idleResource)
        print("resourcePstate", self.resourcePstate)
        print("\n\n")'''



    def onSimulationBegins(self):
        pass

    def onSimulationEnds(self):
        pass

    def onJobSubmission(self, job):
        print("+++ QBox temp diff", self.temperatureDiffs)
        (index, heating) = self.getMaxHeating()

        if index == -1: #No QRads needs heating, reject the job. (TODO in the future the job will be added in a waiting queue)
            print("-------\n",self.temperatureDiffs, "\n")
            self.qnode.onQBoxRejectJob(job, self.qbox_id)
        else:
            if self.idleResource[index] == 1: # This resource is computing a CPU burn job, kill it
                job_id = self.cpuBurn[index]
                self.bs.kill_jobs([self.bs.jobs[job_id]])
                print("------ Just asked to kill the job", job_id, "on machine", index)

            print("------ Execute job", job.id, "on machine", index)
            job.allocation = ProcSet(index)
            self.bs.execute_jobs([job])
            self.idleResource[index] = 0
            self.processingJobs[job.id] = index
            if heating > 1.5: #if heating more than 1.5 degrees required, set machine to full speed
                if self.resourcePstate[index] != 0:
                    self.stateChanges[0] |= ProcSet(index)
                    #self.addStateChange(index, 0)
            else: # set machine to lowest speed
                if self.resourcePstate[index] != (self.qrads_properties[index]["nb_pstates"] -1):
                    #self.addStateChange(index, self.qrads_properties[index]["nb_pstates"]-1)
                    self.stateChanges[self.qrads_properties[index]["nb_pstates"] -1] |= ProcSet(index)

            # assume that the job will heat for 1 degree
            self.temperatureDiffs[index] -= 1.0 
            self.qnode.heat_requirements[self.qbox_id] -= 1.0

    def onJobCompletion(self, job):
        index = self.processingJobs.pop(job.id)
        if job.id.split("!")[0] == "dyn": # This was a CPU burn job
            del self.cpuBurn[index]
            if job.job_state != Job.State.COMPLETED_KILLED: # Only if the CPU burn was not killed
                self.idleResource[index] = 2
        else:
            self.idleResource[index] = 2


    def onJobsKilled(self, jobs):
        pass
        '''for job in jobs:
            if job.id.split("!")[0] == "dyn" and job.id in self.processingJobs: # This was a CPU burn job
                del self.processingJobs[job.id]
                # Do not need to edit self.cpuBurn and self.idleResource since 
                # it SHOULD have been modified when requesting kill of this job'''

    def onNoMoreEvents(self):
        #populate stateChanges with machines that are idle but the pstate is not the last one
        to_sched = []

        print("-----------No more events")

        for (index, run_state) in self.idleResource.items():
            idleState = self.qrads_properties[index]["nb_pstates"]-1
            
            # if the machine needs heating, submit a CPU burn job
            if self.qnode.dynamic_submission_enabled and (run_state == 2) and (self.temperatureDiffs[index] > 0.5):
                job = self.generateSubmitBurnJob()
                job.allocation = ProcSet(index)
                to_sched.append(job)
                self.idleResource[index] = 1
                self.processingJobs[job.id] = index
                self.cpuBurn[index] = job.id
                self.qnode.jobs_mapping[job.id] = self
                if self.resourcePstate[index] != 0:
                    self.stateChanges[0] |= ProcSet(index)
            elif (run_state == 2) and (self.resourcePstate[index] != idleState):
                self.stateChanges[idleState] |= ProcSet(index)
                self.resourcePstate[index] = idleState

        self.bs.execute_jobs(to_sched)
        # Append all SET_RESOURCE_STATE events that occurred during this scheduling phase.
        for (key,value) in self.stateChanges.items():
            self.bs.set_resource_state(value, key)
        self.stateChanges.clear()


    # takes the temperature of the room and compute amount of work needed for each QRad.
    # for now just compute the difference between required and actual temperature
    def updateHeatingReq(self):
        summ = 0.0
        for qr_id in self.list_qrads:
            self.temperatureDiffs[qr_id] = self.requiredTemp - self.bs.air_temperatures[str(qr_id)]
            if self.temperatureDiffs[qr_id] > 0.0:
                summ+= self.temperatureDiffs[qr_id]
        self.qnode.heat_requirements[self.qbox_id] = summ

    # Internal function returning the index and temperature diff of the resource that most required heating
    def getMaxHeating(self):
        maxh = 0.0
        ih = -1
        for (index, heating) in self.temperatureDiffs.items():
            if (self.idleResource[index] > 0) and (maxh < heating):
                ih = index
                maxh = heating

        return (ih, maxh)
        # if return value is -1 then no idle QRad needs to heat

    # Internal function to add to stateChanges another machine.
    '''def addStateChange(self, index, state):
        if state in self.stateChanges:
            self.stateChanges[state] |= ProcSet(index)
        else:
            self.stateChanges[state] = ProcSet(index)'''


    def generateSubmitBurnJob(self):
        job_id = "dyn!qb" + str(self.qbox_id) + "_" + str(self.nextDynId)
        self.nextDynId += 1
        self.bs.register_job(id=job_id, res=1, walltime=-1, profile_name="burn")
        return self.bs.jobs[job_id]