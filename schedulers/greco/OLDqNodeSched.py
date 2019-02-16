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

    def initQBoxesAndStrorageController(self):
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
            opts_dict["qrad_tuple"] = list_qr # Contains a list of pairs (qrad_id, qrad_properties)
            
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

        self.nb_qboxes = len(self.dict_qboxes)
        self.nb_resources = len(self.dict_resources)
        print("-----In QNode Init: Nb_qboxes {}, nb_resources {}, batsim resources {}\n".format(
                    self.nb_qboxes, self.nb_resources, self.bs.nb_resources))


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
        print("QNode end of SimuBegins")

    def onSimulationEnds(self):
        for qb in self.dict_qboxes.values():
            qb.onSimulationEnds()
        self.storage_controller.onSimulationEnds()

    def onJobSubmission(self, job):
        #forward job to one QBox that needs most heating
        self.waiting_jobs.append(job)
        self.tryAndSubmitJobs()

    def onJobCompletion(self, job):
        # forward event to the related QBox
        print("--- Job", job.id, "completed!")
        
        if "dyn-staging" in job.id:
            self.storage_controller.onDataStagingCompletion(job)
        else:
            qb = self.jobs_mapping.pop(job.id)
            qb.onJobCompletion(job)

    def onJobsKilled(self, jobs):
        pass

    def onRequestedCall(self):
        self.notify_all_registration_finished()
        #pass
        #TODO see if needed to forward to all QBoxes

    def onMachinePStateChanged(self, nodeid, pstate):
        pass
        #TODO see if needed to forward toa ll QBoxes

    def onBeforeEvents(self):
        #print("\n[", self.bs.time(), "] new Batsim message")
        for qb in self.dict_qboxes.values():
            qb.onBeforeEvents()
        print("After beforeevents heating req:", self.heat_requirements)


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

    # Internal function returning the QBox that needs most heating
    def getMaxHeatingReq(self):
        maxh = 0.0
        ih = -1
        print("QNode heat requirements", self.heat_requirements)
        for (index, heating) in self.heat_requirements.items():
            if (maxh < heating):
                ih = index
                maxh = heating

        return (ih, maxh)
        # if return value is -1 then no QBox needs heating

    # Internal function that dispatches jobs in the waiting queue on QBoxes that needs most heating
    def tryAndSubmitJobs(self):
        #print("--- QNode heating requirements:", self.heat_requirements)
        flag = True
        while flag:
            (index, heating) = self.getMaxHeatingReq()
            if (index == -1) or (len(self.waiting_jobs) == 0):
                # No QBox needs heating or there is no job in the queue
                print("No job dispatched", index, len(self.waiting_jobs))
                flag = False
            else:
                # Schedule the job
                job = self.waiting_jobs.pop(0)
                qb = self.dict_qboxes[index]
                self.jobs_mapping[job.id] = qb
                # TODO see if we reschedule the job to another qbox or we use the onQBoxRejectJob call
                print("QNode sent job", job.id, "to QBox", index)
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

