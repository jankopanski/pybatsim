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
        self.candidadates_qb = {}

    def initQBoxesAndStrorageController(self):
        print("Heeeeeeeeeeeeeeeeey -------\n")
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

        print("\n********************* ", self.dict_qboxes['1'].disk_id)
        
        #TODO: Add some dataset in some QBox to do the experiment
        self.storage_controller.add_dataset(self.dict_qboxes['0'].disk_id, Dataset("ds3", 17500))
        self.storage_controller.add_dataset(self.dict_qboxes['1'].disk_id, Dataset("ds3, ds2", 15000))
        

        self.nb_qboxes = len(self.dict_qboxes)
        self.nb_resources = len(self.dict_resources)
        print("-----In QNode Init: Nb_qboxes {}, nb_resources {}, batsim resources {}\n".format(
                    self.nb_qboxes, self.nb_resources, self.bs.nb_resources))

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
            # required_datasets[job.profile] has the list of datasets, here we are looking just for the first,
            # assuming that we have only one.
            if (storage._datasets.get(required_datasets[job.profile][0]) != None):
                print("     This QBOX has the required dataset. QBOX: ", self.storage_controller.mappingQBoxes[storage._id])
                candidadate_qb[self.storage_controller.mappingQBoxes[storage._id]] = required_datasets[job.profile]
        print("--------------------------------------------------------------------------")
        print("List of candidate qboxes: ", candidadate_qb)
        print("--------------------------------------------------------------------------")
        self.candidadates_qb[job] = candidadate_qb

    def schedule(self, job):

        print("------ Working on the list of QBoxes that already have some specific datset -------\n")
        print("Job_ID: ", job.id)
        print("Job_Profile: ", job.profile)
        print("----------------------------------")
        self.list_qboxes_with_datasets(job)
                 


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
        pass
        #TODO

    def onSimulationEnds(self):
        pass
        #TODO

    def onJobSubmission(self, job):
        print("-------------------------------------\n")
        self.schedule(job)
        #TODO

    def onJobCompletion(self, job):
        pass
        #TODO

    def onJobsKilled(self, jobs):
        pass
        #TODO

    def onRequestedCall(self):
        pass
        #TODO
    def onMachinePStateChanged(self, nodeid, pstate):
        pass
        #TODO see if needed to forward toa ll QBoxes

    def onBeforeEvents(self):
        pass
        #TODO

    def onNoMoreEvents(self):
        pass
        #TODO
    # Internal function used by QBoxes to reject a job and return it to the QNode scheduler
    def onQBoxRejectJob(self, job, qbox_id):
        pass
        #TODO
    # Internal function returning the QBox that needs most heating
    def getMaxHeatingReq(self):
        pass
        #TODO

    # Internal function that dispatches jobs in the waiting queue on QBoxes that needs most heating
    def tryAndSubmitJobs(self):
        pass
        #TODO

    def notify_all_registration_finished(self):
        pass
        #TODO

    def onNotifyEventTargetTemperatureChanged(self, machines, new_temperature):
        pass
        #TODO
    def onNotifyEventNewDatasetOnStorage(self, machines, dataset_id, dataset_size):
        pass
        #TODO

    def onNotifyEventMachineUnavailable(self, machines):
        pass
        #TODO
    def onNotifyEventMachineAvailable(self, machines):
        pass
        #TODO

