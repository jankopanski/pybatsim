from batsim.batsim import BatsimScheduler, Batsim, Job
from batsim.tools.launcher import launch_scheduler, instanciate_scheduler
from StorageController import *
from collections import defaultdict

import sys
import os
import logging


''' Here the master sched plays two roles:
    - first the interface between Batsim and the QNode/QBox schedulers
    - second it is itself the QNode scheduler
'''
'''
    Workload example
    
    "nb_res": 1000,
        "jobs": [
            {
                "id": "QJOB-0206-0100-da09-6ceed368695c-0",
                "profile": "QJOB-0206-0100-da09-6ceed368695c-0-profile",
                "res": 1,
                "subtime": 0
            }, 
        ...
        ]
        "QJOB-0206-0702-e8a3-b383e8952792-0-profile": {
            "priority": "normal-user",
            "user": "unknown-user",
            "type": "parallel_homogeneous",
            "cpu": 161.798736572266,
            "com": 0,
            "datasets": [
                "QJOB-0206-0702-e8a3-b383e8952792:user-input:0",
                "QJOB-0206-0702-e8a3-b383e8952792:docker:103846696",
                "QJOB-0206-0702-e8a3-b383e8952792:user-input:41428162"
            ]
        },
        ...

'''

class QNodeSched(BatsimScheduler):
    def __init__(self, options):
        super().__init__(options)
        
        if not "qbox_sched" in options:
            print("No qbox_sched provided in json options, using qBoxSched by default.")
            self.qbox_sched_name = "schedulers/greco/qBoxSched.py"
        else:
            self.qbox_sched_name = options["qbox_sched"]

        self.dict_qboxes = {}    # Maps the QBox id to the QBoxSched object
        self.dict_resources = {} # Maps the batsim resource id to the QBoxSched object
        self.heat_requirements = {}
        self.jobs_mapping = {}        # Maps the job id to the qbox id where it has been sent to
        self.waiting_jobs = []
        self.candidates_qb = {}

    def initQBoxesAndStrorageController(self):
        print("------- Initializing the QBoxes and the StorageController -------\n")
        # Read the platform file to have the architecture.
        dict_ids = defaultdict(list)
        # Retrieves the QBoxe ids and the associated list of QRad ids
        for res in self.bs.machines["compute"]:
            batsim_id = res["id"]
            names = res["name"].split("_")
            qb_id = names[0]#.lstrip("qb")
            qr_id = names[1]#.lstrip("qr")
            properties = res["properties"]

            dict_ids[qb_id].append((batsim_id, properties))

        # loops over the QBoxes in the platform
        for (qb_id, list_qr) in dict_ids.items():
            opts_dict = {}
            opts_dict["qbox_id"] = qb_id
            opts_dict["resource_ids"] = list_qr # Contains a list of pairs (qrad_id, qrad_properties)
            
            qb = instanciate_scheduler(self.qbox_sched_name, opts_dict)
            qb.qnode = self
            qb.bs = self.bs

            self.dict_qboxes[qb_id] = qb
            self.heat_requirements[qb_id] = 0

            # Populate the mapping between batsim resource ids and the id of the associated QBox
            for qr_tuple in list_qr:
                self.dict_resources[qr_tuple[0]] = qb

        self.storage_controller = StorageController(self.bs)
        for machine in self.bs.machines["storage"]:
            self.storage_controller.add_storage(Storage(machine["id"], machine["name"], float(machine["properties"]["size"])))
            if machine["name"] == "storage_server":
                self.ceph_id = machine["id"]
                self.storage_controller._ceph_id = machine["id"]
            else:
                # Each QBox has to know the resource id of its own disk
                qb_id = machine["name"].rstrip("_disk")
                self.dict_qboxes[qb_id].disk_id = machine["id"]
                self.storage_controller.mappingQBoxes[machine["id"]] = self.dict_qboxes[qb_id]

        self.storage_controller.add_dataset(self.ceph_id, Dataset("d1", 100000))
        self.storage_controller.add_dataset(self.ceph_id, Dataset("d2", 30000))       
        #TODO: Add some dataset in some QBox to do the experiment
        self.storage_controller.add_dataset(self.dict_qboxes['0'].disk_id, Dataset("ds2", 175))
        self.storage_controller.add_dataset(self.dict_qboxes['1'].disk_id, Dataset("ds2", 150))
        self.storage_controller.add_dataset(self.dict_qboxes['1'].disk_id, Dataset("ds3", 150))
        
        self.storage_controller.add_dataset(self.dict_qboxes['0'].disk_id, Dataset("QJOB-0206-0100-da09-6ceed368695c:user-input:41428146", 17))
        self.storage_controller.add_dataset(self.dict_qboxes['0'].disk_id, Dataset("QJOB-0206-0100-da09-6ceed368695c:docker:0", 17))
        self.storage_controller.add_dataset(self.dict_qboxes['0'].disk_id, Dataset("QJOB-0206-0100-da09-6ceed368695c:user-input:0", 17))
        print("---Qboxes have a life :) ")

        self.nb_qboxes = len(self.dict_qboxes)
        self.nb_resources = len(self.dict_resources)
        print("-----In QNode Init: Nb_qboxes {}, nb_resources {}, batsim resources {}\n".format(
                    self.nb_qboxes, self.nb_resources, self.bs.nb_resources))

    # TODO: At the end, put it on the StoraController.py 
    def list_qboxes_with_datasets(self, job):
        """ Lists all QBoxes that has the required list of datasets from the job """

        required_datasets = {} # To get the list of datasets requireds by the job
        candidadate_qb = {} # The dict of all qboxes with the required dataset

        # To get the datasets required per a job, we need to search the profile in the bs.profiles.
        # So, we search in the bs.profiles the job.profile.
        # The bs.profile is the format: {ID : {pf1: "descrip. pf1", pf2: "descrip. pf2, ...}}
        for key in self.bs.profiles:
            # Here we are in the ID level, so, now we have a dict as {pf1: "descrip. pf1", pf2: "descrip. pf2, ...}
            if (self.bs.profiles[key].get(job.profile) != None) :
                # To save like : required_datasets = {"job.profile" : "datasets"}
                required_datasets[job.profile] = self.bs.profiles[key][job.profile]['datasets']
        print("--------------------------------------------------------------------------")
        print("Job: {} Requires (profile, dataset): {}" .format(job.id.split("!")[1], required_datasets))

        # To check the storages
        for storage in self.storage_controller.get_storages():
            # At this point, storage is an ID. Let's take the storage (Class) with this ID.
            storage = self.storage_controller.get_storage(storage)
            print("{} storages: {} " .format(storage._name, storage._datasets))
            hasDataset = True
            # To check if this storage has all the datasets required.
            for dataset in required_datasets[job.profile]:
                if (storage._datasets.get(dataset) == None):
                    hasDataset = False
                    break
            # Here, the storage has all required datasets
            # candidate_qb = {storage : datasets}
            if(hasDataset):
                print("     This QBOX has the required dataset. QBOX: ", self.storage_controller.mappingQBoxes[storage._id])
                candidadate_qb[self.storage_controller.mappingQBoxes[storage._id]] = required_datasets[job.profile]
        
        # candidates_qb = {job: {storage : datasets}}
        if (len(candidadate_qb) != 0) : 
            self.candidates_qb[job] = candidadate_qb
            print("--------------------------------------------------------------------------")
            print("List of candidate qboxes: ", self.candidates_qb[job])
            print("--------------------------------------------------------------------------")
    
    def getMaxHeatingReq(self):
        maxh = 0.0
        ih = -1
        for (index, heating) in self.heat_requirements.items():
            if (maxh < heating):
                ih = index
                maxh = heating
        return (ih, maxh)
        # if return value is -1 then no QBox needs heating

    def getMaxHeatingReq_inList(self, job):
        """ Receives a job and check which qbox on the candidates_qbox requires the highest value for heating. 
        It returns this qbox """
        
        maxh = 0.0
        qboxh = None
        for qb in self.candidates_qb[job]:
            print("   Possible QBox : ", qb)
            heating = self.heat_requirements.get(qb)
            print("   Requires heating: ", heating)
            if (heating != None and heating > maxh):
                print("   Here!")
                qboxh = qb
                maxh = heating # maybe I will use it, not now
        return qboxh

    def getTasksFromJob(self):
        """ To get the indexes of jobs in the waiting list that composes the same main job"""
        for task in self.waiting_jobs :
            print(task.id)
            task = task.id.split("!")[1]
            print(task)
            task = task.split("-")
            print(task)

    def schedule(self, job):
        print("------ Working on the list of QBoxes that already have some specific datset -------\n")
        print("Job_ID: ", job.id)
        print("Job_Profile: ", job.profile)
        print("----------------------------------")
        
        # To construct the list L
        print(" 1 ==================================")
        self.list_qboxes_with_datasets(job)
        print(self.candidates_qb)
        
        # To check if there is some qbox to run the job, if more than one, select the best one.
        print(" 2 ==================================")
        self.getTasksFromJob()

        print(" 2.1 ==================================")
        if (self.candidates_qb.get(job) != None):
            print(" >>>> Can run ")
            selected_qbox = self.getMaxHeatingReq_inList(job)
            if (selected_qbox != None):
                print("  Selected QBox: ", selected_qbox)
                self.jobs_mapping[selected_qbox] = job # To map the Job on the QBox
                # Dispatching the jobs
                print(" 3 ==================================")
                #self.dict_qboxes[selected_qbox].onJobSubmission(job)
            else:
                # Dispatch on the first Qbox of the list L
                selected_qbox = list(self.candidates_qb.get(job).keys())[0]
                print("  Selected QBox: ", selected_qbox)
                self.jobs_mapping[selected_qbox] = job # To map the Job on the QBox
                # Dispatching the jobs
                print(" 3 ==================================")
                #self.dict_qboxes[selected_qbox.qbox_id].onJobSubmission(job)

    def onSimulationBegins(self):
        #self.bs.logger.setLevel(logging.ERROR)
        profile = {
            "burn" : {'type' : 'parallel_homogeneous', 'cpu' : 1e10, 'com' : 0}
        }
        self.bs.register_profiles("dyn", profile)
        self.bs.wake_me_up_at(1000)

        self.dynamic_submission_enabled = self.bs.dynamic_job_registration_enabled

        self.initQBoxesAndStrorageController()
        for qb in self.dict_qboxes.values():
            qb.onBeforeEvents()
            qb.onSimulationBegins()
        #pass
        #TODO

    def onSimulationEnds(self):
        for qb in self.dict_qboxes.values():
            qb.onSimulationEnds()
        self.storage_controller.onSimulationEnds()
        #TODO

    def onJobSubmission(self, job):
        print(" >> New Job Submitted -------------------------------------\n")
        self.waiting_jobs.append(job)
        self.schedule(job)
        #TODO

    def onJobCompletion(self, job):
        # forward event to the related QBox
        print("--- Job", job.id, "completed!")
        
        if "dyn-staging" in job.id:
            self.storage_controller.onDataStagingCompletion(job)
        else:
            qb = self.jobs_mapping.pop(job.id)
            qb.onJobCompletion(job)
        #TODO

    def onJobsKilled(self, jobs):
        pass
        #TODO

    def onRequestedCall(self):
        self.notify_all_registration_finished()
        #pass
        #TODO
    def onMachinePStateChanged(self, nodeid, pstate):
        pass
        #TODO see if needed to forward toa ll QBoxes

    def onBeforeEvents(self):
        for qb in self.dict_qboxes.values():
            qb.onBeforeEvents()
        #pass
        #TODO

    def onNoMoreEvents(self):
        for qb in self.dict_qboxes.values():
            qb.onNoMoreEvents()
        print("---QNode after NoMoreEvents")
        '''if(self.bs.time() < 1e4):
            self.bs.wake_me_up_at(self.bs.time()+300)
        else:
            self.bs.notify_submission_finished()'''

    # Internal function used by QBoxes to reject a job and return it to the QNode scheduler
    def onQBoxRejectJob(self, job, qbox_id):
        #TODO for now the job is just rejected to batsim.
        # In the future, send the job to another qbox.
        del self.jobs_mapping[job.id]
        self.bs.reject_jobs([job])
        #TODO KEEP THE JOB IN THE WAITING QUEUE, OR GIVE IT TO ANOTHER QRAD, but it cannot just be rejected to the user (aka Batsim here)

    # Internal function that dispatches jobs in the waiting queue on QBoxes that needs most heating
    def tryAndSubmitJobs(self):
        print("--- QNode heating requirements:", self.heat_requirements)
        flag = True
        while flag:
            (index, heating) = self.getMaxHeatingReq()
            if (index == -1) or (len(self.waiting_jobs) == 0):
                # No QBox needs heating or there is no job in the queue
                flag = False
            else:
                # Schedule the job
                job = self.waiting_jobs.pop(0)
                qb = self.dict_qboxes[index]
                self.jobs_mapping[job.id] = qb
                # TODO see if we reschedule the job to another qbox or we use the onQBoxRejectJob call
                qb.onJobSubmission(job)

    def notify_all_registration_finished(self):
        self.bs.notify_registration_finished()
        self.dynamic_submission_enabled = False


    def onNotifyEventTargetTemperatureChanged(self, machines, new_temperature):
        for machine_id in machines:
            self.dict_resources[machine_id].onTargetTemperatureChanged(machine_id, new_temperature)

    def onNotifyEventNewDatasetOnStorage(self, machines, dataset_id, dataset_size):
        self.storage_controller.onNotifyEventNewDatasetOnStorage(machines, dataset_id, dataset_size)


    def onNotifyEventMachineUnavailable(self, machines):
        for machine_id in machines:
            self.dict_resources[machine_id].onNotifyMachineUnavailable(machine_id)

    def onNotifyEventMachineAvailable(self, machines):
        for machine_id in machines:
            self.dict_resources[machine_id].onNotifyMachineAvailable(machine_id)

