from batsim.batsim import BatsimScheduler, Batsim, Job
from collections import defaultdict
from procset import ProcSet

import sys
import os
import logging

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

        # TODO remove the default value of the targetTemperatures?
        self.targetTemperatures = defaultdict(lambda:20) # Stores the target temperature of each qrad, set to 20 by default
        self.diffTemperatures = {} # Stores the temperature difference b/w targetTemperatures and actual airTemp for each resource.

        self.nextDynId = 0         # Index of next dynamic job

        #self.jobQueue = []                                          # stores the jobs waiting to be scheduled (or rejected)
        self.processingJobs = {}                                    # maps the job id that is running to the resource id
        self.cpuBurn = {}                                           # maps the resource id to the job id
        self.idleResource = {x:2 for x in self.list_qrads}          # whether the resource is running a job (0), a CPU burn (1) or idle (2)
        self.resourcePstate = {x:0 for x in self.list_qrads}        # maps the resource id to its current Pstate
        self.unavailableMachines = ProcSet()

        self.waitingDatasets = {}
        self.waitingToLaunch = {}
        self.datasetMapping = defaultdict(list)

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
        print("+++ QBox temp diff", self.diffTemperatures)
        (index, heating) = self.getMaxHeating()

        if index == -1: #No QRads needs heating, reject the job. (TODO in the future the job will be added in a waiting queue)
            print("-------\n",self.diffTemperatures, "\n")
            self.qnode.onQBoxRejectJob(job, self.qbox_id)
        else:
            # Need to rework all this with taking into account the storage controller
            if self.idleResource[index] == 1: # This resource is computing a CPU burn job, kill it
                job_id = self.cpuBurn[index]
                self.bs.kill_jobs([self.bs.jobs[job_id]])
                print("------ Just asked to kill the job", job_id, "on machine", index)

            print("[", self.bs.time(), "]------ Execute job", job.id, "on machine", index)
            job.allocation = ProcSet(index)
            #self.bs.execute_jobs([job])
            datasets = job.json_dict["datasets"]
            self.waitingDatasets[job.id] = datasets
            self.waitingToLaunch[job.id] = job
            for dataset_id in datasets:
                self.datasetMapping[dataset_id].append(job.id)
            print("[", self.bs.time(), "] For job", job.id, str(job.allocation), "asking datasets", datasets, " to qbox disk", self.disk_id, self.qbox_id)
            self.qnode.storage_controller.move_to_dest(datasets, self.disk_id)

            # assume that the job will heat for 1 degree
            self.diffTemperatures[index] -= 1.0
            self.qnode.heat_requirements[self.qbox_id] -= 1.0

    def onJobCompletion(self, job):
        resource_index = self.processingJobs.pop(job.id)
        if job.id.split("!")[0] == "dyn": # This was a CPU burn job
            del self.cpuBurn[resource_index]
            if job.job_state != Job.State.COMPLETED_KILLED: # Only if the CPU burn was not killed
                self.idleResource[resource_index] = 2
        else:
            self.idleResource[resource_index] = 2
            for dataset_id in job.json_dict["datasets"]:
                self.datasetMapping[dataset_id].remove(job.id)




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

        for (index, run_state) in self.idleResource.items():
            if index in self.unavailableMachines:
                continue

            idleState = self.qrads_properties[index]["nb_pstates"]-1
            
            # if the machine needs heating, submit a CPU burn job
            if self.qnode.dynamic_submission_enabled and (run_state == 2) and (self.diffTemperatures[index] > 0.5):
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


    def generateSubmitBurnJob(self):
        job_id = "dyn!qb" + str(self.qbox_id) + "_" + str(self.nextDynId)
        self.nextDynId += 1
        self.bs.register_job(id=job_id, res=1, walltime=-1, profile_name="burn")
        return self.bs.jobs[job_id]


    def onDatasetArrived(self, dataset_id):
        print("[", self.bs.time(), "] Dataset", dataset_id, "just arrived!")
        for job_id in self.datasetMapping[dataset_id]:
            if job_id in self.waitingDatasets and dataset_id in self.waitingDatasets[job_id]:
                self.waitingDatasets[job_id].remove(dataset_id)
                if len(self.waitingDatasets[job_id]) == 0:
                    del self.waitingDatasets[job_id]
                    self.launchJob(job_id)

    def launchJob(self, job_id):
        job = self.waitingToLaunch.pop(job_id)
        print("[", self.bs.time(), "] Asking to launch job", job.id, "on machine", str(job.allocation))

        print(self.diffTemperatures)

        self.bs.execute_jobs([job])
        index = list(job.allocation)[0]
        heating = self.diffTemperatures[index]
        self.idleResource[index] = 0
        self.processingJobs[job.id] = index
        if heating > 1.5: #if heating more than 1.5 degrees required, set machine to full speed
            if self.resourcePstate[index] != 0:
                self.stateChanges[0] |= ProcSet(index)
        else: # set machine to lowest speed
            if self.resourcePstate[index] != (self.qrads_properties[index]["nb_pstates"] -1):
                self.stateChanges[self.qrads_properties[index]["nb_pstates"] -1] |= ProcSet(index)


    def onNotifyMachineUnavailable(self, machine_id):
        self.unavailableMachines |= ProcSet(machine_id)

    def onNotifyMachineAvailable(self, machine_id):
        self.unavailableMachines -= ProcSet(machine_id)



# HEAT and TEMPERATURE related methods

    # takes the temperature of the room and compute amount of work needed for each QRad.
    # for now just compute the difference between required and actual temperature
    def updateHeatingReq(self):
        summ = 0.0
        for qr_id in self.list_qrads:
            self.diffTemperatures[qr_id] = self.targetTemperatures[qr_id] - self.bs.air_temperatures[str(qr_id)]
            if self.diffTemperatures[qr_id] > 0.0:
                summ+= self.diffTemperatures[qr_id]
        self.qnode.heat_requirements[self.qbox_id] = summ

    # Internal function returning the index and temperature diff of the available resource that most requires heating
    def getMaxHeating(self):
        maxh = 0.0
        ih = -1
        for (index, heating) in self.diffTemperatures.items():
            if index not in self.unavailableMachines and (self.idleResource[index] > 0) and (maxh < heating):
                ih = index
                maxh = heating

        return (ih, maxh)
        # if return value is -1 then no idle QRad needs to heat

    # Handler of a NOTIFY event from Batsim: updates the target temperature of a machine
    def onTargetTemperatureChanged(self, machine, new_temperature):
        self.targetTemperatures[machine] = new_temperature
        self.diffTemperatures[machine] = new_temperature - self.bs.air_temperatures[str(machine)]






