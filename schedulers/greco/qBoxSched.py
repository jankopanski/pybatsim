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
        self.options = options
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


        #self.jobQueue = []                                          # stores the jobs waiting to be scheduled (or rejected)
        self.processingJobs = {}                                    # maps the job id that is running to the resource id
        self.idleResource = {x:True for x in self.list_qrads}       # whether the resource is idle
        self.resourcePstate = {x:0 for x in self.list_qrads}        # maps the resource id to its current Pstate

        # keys are the target states, values are lists of resources to which to change the state
        self.stateChanges = defaultdict(list)

    def onBeforeEvents(self):
        self.updateHeatingReq()



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
            job.allocation = ProcSet(index)
            self.bs.execute_jobs([job])
            self.idleResource[index] = False
            self.processingJobs[job.id] = index
            if heating > 1.5: #if heating more than 1.5 degrees required, set machine to full speed
                if self.resourcePstate[index] != 0:
                    self.stateChanges[0].append(index)
                    #self.addStateChange(index, 0)
            else: # set machine to lowest speed
                if self.resourcePstate[index] != (self.qrads_properties[index]["nb_pstates"] -1):
                    #self.addStateChange(index, self.qrads_properties[index]["nb_pstates"]-1)
                    self.stateChanges[self.qrads_properties[index]["nb_pstates"] -1].append(index)

            # assume that the job will heat for 1 degree
            self.temperatureDiffs[index] -= 1.0 
            self.qnode.heat_requirements[self.qbox_id] -= 1.0

    def onJobCompletion(self, job):
        self.idleResource[self.processingJobs[job.id]] = True
        del self.processingJobs[job.id]



    def onNoMoreEvents(self):

        #populate stateChanges with machines that are idle but the pstate is not the last one
        for (index, idle) in self.idleResource.items():
            idleState = self.qrads_properties[index]["nb_pstates"]-1
            if idle and (self.resourcePstate[index] != idleState):
                self.stateChanges[idleState].append(index)
                self.resourcePstate[index] = idleState

        # Append all SET_RESOURCE_STATE events that occurred during this scheduling phase.
        for (key,value) in self.stateChanges.items():
            self.bs.set_resource_state(value, key)
        self.stateChanges = defaultdict(list)


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
            if (self.idleResource[index]) and (maxh < heating):
                ih = index
                maxh = heating

        return (ih, maxh)
        # if return value is -1 then no idle QRad needs to heat

    # Internal function to add to stateChanges another machine.
    '''def addStateChange(self, index, state):
        if state in self.stateChanges:
            self.stateChanges[state].append(index)
        else:
            self.stateChanges[state] = [index]'''
