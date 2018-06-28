from batsim.batsim import BatsimScheduler, Batsim, Job
from batsim.tools.launcher import launch_scheduler, instanciate_scheduler

import sys
import os
import logging

''' Here the master sched plays two roles:
    - first the interface between Batsim and the QNode/QBox schedulers
    - second it is itself the QNode scheduler
'''
class QNodeSched(BatsimScheduler):
    def __init__(self, options):
        self.options = options

        if not "qbox_sched" in options:
            print("No qbox_sched provided in json options, aborting.")
            sys.exit(1)
        else:
            self.qbox_sched_name = options["qbox_sched"]

        self.dict_qboxes = {}    # Maps the QBox id to the QBoxSched object
        self.dict_resources = {} # Maps the batsim resource id to the QBox associated
        self.heat_requirements = {}
        self.jobs_mapping = {}        # Maps the job id to the qbox id where it has been sent to
        self.waiting_jobs = []

        self.received_begin = False

    def onAfterBatsimInit(self):
        # Read the platform file to have the architecture.
        dict_ids = {}
        # Retrieves the QBoxe ids and the associated list of QRad ids
        for res in self.bs.machines["compute"]:
            batsim_id = res["id"]
            names = res["name"].split("_")
            qb_id = names[0].lstrip("qb")
            qr_id = names[1].lstrip("qr")
            properties = res["properties"]

            if not qb_id in dict_ids:
                dict_ids[qb_id] = [(batsim_id, properties)]
            else:
                dict_ids[qb_id].append((batsim_id, properties))

        # loops over the QBoxes in the platform
        for (qb_id, list_qr) in dict_ids.items():
            opts_dict = {}
            opts_dict["qbox_id"] = qb_id
            opts_dict["resource_ids"] = list_qr # Contains a list of pairs (qrad_id, qrad_properties)
            #TODO PUT SOMEWHERE HERE THE TEMPERATURE REQUIREMENTS TO GIVE TO THE QBOX SCHED
            # Maybe put the temperature schema filename in the SG xml platform description
            
            qb = instanciate_scheduler(self.qbox_sched_name, opts_dict)
            qb.qnode = self
            qb.bs = self.bs

            self.dict_qboxes[qb_id] = qb
            self.heat_requirements[qb_id] = 0

            # Populate the mapping between batsim resource ids and the id of the associated QBox
            for qr_id in list_qr:
                self.dict_resources[str(qr_id)] = qb_id

        self.nb_qboxes = len(self.dict_qboxes)
        self.nb_resources = len(self.dict_resources)
        print("-----In QNode Init: Nb_qboxes {}, nb_resources {}, batsim resources {}\n".format(
                    self.nb_qboxes, self.nb_resources, self.bs.nb_resources))


    def onSimulationBegins(self):
        #self.bs.logger.setLevel(logging.ERROR)
        self.received_begin = True
        for qb in self.dict_qboxes.values():
            qb.onSimulationBegins()

        #TODO send profile of CPU burn jobs

    def onSimulationEnds(self):
        for qb in self.dict_qboxes.values():
            qb.onSimulationEnds()

    def onJobSubmission(self, job):
        #forward job to one QBox that needs most heating
        self.waiting_jobs.append(job)
        self.tryAndSubmitJobs()

    def onJobCompletion(self, job):
        # forward event to the related QBox
        
        #WIP
        qb = self.jobs_mapping[job.id]
        qb.onJobCompletion(job)
        del self.jobs_mapping[job.id]

    def onRequestedCall(self):
        pass
        #TODO see if needed to forward to all QBoxes

    def onMachinePStateChanged(self, nodeid, pstate):
        pass
        #TODO see if needed to forward toa ll QBoxes

    def onBeforeEvents(self):
        if self.received_begin:
            for qb in self.dict_qboxes.values():
                qb.onBeforeEvents()


    def onNoMoreEvents(self):
        for qb in self.dict_qboxes.values():
            qb.onNoMoreEvents()
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

    # Internal function returning the QBox that needs most heating
    def getMaxHeatingReq(self):
        maxh = 0.0
        ih = -1
        for (index, heating) in self.heat_requirements.items():
            if (maxh < heating):
                ih = index
                maxh = heating

        return (ih, maxh)
        # if return value is -1 then no QBox needs heating

    # Internal function that dispatches jobs in the waiting queue on QBoxes that needs most heating
    def tryAndSubmitJobs(self):
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