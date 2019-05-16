from batsim.batsim import BatsimScheduler, Batsim, Job
from StorageController import StorageController
from qarnotUtils import *
from qarnotBoxSched import QarnotBoxSched
from qarnotNodeSched import QarnotNodeSched

from procset import ProcSet
from collections import defaultdict
from copy import deepcopy

import math
import logging
import sys
import os

import csv
import json

'''
This is a variant of the qarnotNodeSched that takes into account locality of the datasets
to dispatch instances.
'''

class QarnotNodeSchedAndrei(QarnotNodeSched):
    def __init__(self, options):
        super().__init__(options)

<<<<<<< HEAD
        #self.logger.setLevel(logging.DEBUG)

        # Make sure the path to the datasets is passed
        assert "input_path" in options, "The path to the input files should be given as a CLI option as follows: [pybatsim command] -o \'{\"input_path\":\"path/to/input/files\"}\'"
        if not os.path.exists(options["input_path"]):
                assert False, "Could not find input path {}".format(options["input_path"])

        if "update_period" in options:
            self.update_period = options["update_period"]
        else:
            #self.update_period = 30 # The scheduler will be woken up by Batsim every 30 seconds
            #self.update_period = 600 # TODO every 10 minutes for testing
            self.update_period = 150 # TODO every 2.5 minutes for testing

        if "output_path" in options:
            self.output_filename = options["output_path"] + "/out_pybatsim.csv"
        else:
            self.output_filename = None

        # For the manager
        self.dict_qboxes = {}        # Maps the QBox id to the QarnotBoxSched object
        self.dict_resources = {} # Maps the Batsim resource id to the QarnotBoxSched object
        self.dict_qrads = {}         # Maps the QRad name to the QarnotBoxSched object
        self.dict_sites = defaultdict(list) # Maps the site name ("paris" or "bordeaux" for now) to a list of QarnotBoxSched object
        self.numeric_ids = {} # Maps the Qmobo name to its batid

        self.qbox_sched_name = QarnotBoxSched
        self.storage_controller_name = StorageController

        # Dispatcher
        self.qtasks_queue = {}     # Maps the QTask id to the QTask object that is waiting to be scheduled
        self.jobs_mapping = {}     # Maps the batsim job id to the QBox Object where it has been sent to
        
        self.lists_available_mobos = [] # List of [qbox_name, slots bkgd, slots low, slots high]
                                        # This list is updated every 30 seconds from the reports of the QBoxes
        self.direct_dispatch_enabled = True # Whehter direct dispatch of intances is possible

        #NOT USED AT THE MOMENT:
        #self.qboxes_free_disk_space = {} # Maps the QBox id to the free disk space (in GB)
        #self.qboes_queued_upload_size = {} # Maps the QBox id to the queued upload size (in GB)

        self.time_next_update = 1.0 # The next time the scheduler should be woken up
        self.update_in_current_step = False # Whether update should be done in this current scheduling step
        self.next_update_asked = False     # Whether the 'call_me_later' has been sent or not
        self.very_first_update = True

        self.next_burn_job_id = 0
        self.nb_rejected_jobs_by_qboxes = 0 # Just to keep track of the count
        self.nb_preempted_jobs = 0

        #self.max_simulation_time = 87100 # 1 day
        self.max_simulation_time = 1211000 # 2 weeks TODO remove this guard?
        self.end_of_simulation_asked = False
        self.next_print = 43200


    def onSimulationBegins(self):
        assert self.bs.dynamic_job_registration_enabled, "Registration of dynamic jobs must be enabled for this scheduler to work"
        assert self.bs.allow_storage_sharing, "Storage sharing must be enabled for this scheduler to work"
        assert self.bs.ack_of_dynamic_jobs == False, "Acknowledgment of dynamic jobs must be disabled for this scheduler to work"
        #assert len(self.bs.air_temperatures) > 0, "Temperature option '-T 1' of Batsim should be set for this scheduler to work"
        # TODO maybe change the option to send every 30 seconds instead of every message of Batsim (see TODO list)

        # Register the profile of the cpu-burn jobs. CPU value is 1e20 because it's supposed to be endless and killed when needed.
        self.bs.register_profiles("dyn-burn", {"burn":{"type":"parallel_homogeneous", "cpu":1e20, "com":0, "priority": -23}})
        self.bs.wake_me_up_at(1)

        self.initQBoxesAndStorageController()
        for qb in self.dict_qboxes.values():
            qb.onBeforeEvents()

        self.storage_controller.onSimulationBegins()

        self.logger.info("[{}]- QNode: End of SimulationBegins".format(self.bs.time()))


    def onSimulationEnds(self):
        #self.simu_ends_received = True
        for qb in self.dict_qboxes.values():
            qb.onSimulationEnds()

        self.storage_controller.onSimulationEnds()

        print("Number of rejected instances by QBoxes during dispatch:", self.nb_rejected_jobs_by_qboxes)
        print("Number of burn jobs created:", self.next_burn_job_id)
        print("Number of staging jobs created:", self.storage_controller._next_staging_job_id)
        print("Number of preempted jobs:", self.nb_preempted_jobs)
        print("Update_period was:", self.update_period)

        if self.output_filename != None:
            self.write_output_to_file()

    def write_output_to_file(self):
        print("Writing outputs to", self.output_filename)
        with open(self.output_filename, 'w', newline='') as csvfile:
            fieldnames = ['update_period', 'nb_rejected_instances_during_dispatch', 'nb_burn_jobs_created', 'nb_staging_jobs_created', 'nb_preempted_jobs']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({
                'update_period': self.update_period,
                'nb_rejected_instances_during_dispatch': self.nb_rejected_jobs_by_qboxes,
                'nb_burn_jobs_created': self.next_burn_job_id,
                'nb_staging_jobs_created': self.storage_controller._next_staging_job_id,
                'nb_preempted_jobs': self.nb_preempted_jobs})


    def initQBoxesAndStorageController(self):
        # Let's create the StorageController
        self.storage_controller = self.storage_controller_name(self.bs.machines["storage"], self.bs, self, self.options["input_path"])

        # Retrieve the QBox ids and the associated list of QMobos Batsim ids
        dict_ids = defaultdict(lambda: defaultdict(list))
        # A dict where keys are qb_names
        # and values are dict where keys are qr_names
        #                        and values are lists of (mobo batid, mobo name, mobo properties)
        for res in self.bs.machines["compute"]:
            batid = res["id"]
            qm_name = res["name"]
            qr_name = res["properties"]["qrad"]
            qb_name = res["properties"]["qbox"]
            properties = res["properties"]
            properties["speeds"] = res["speeds"]

            dict_ids[qb_name][qr_name].append((batid, qm_name, properties))

        # Retrieve the numeric ids given by the extractor. To check that they are the same as batids of the mobos.
        with open(self.options["input_path"]+"/platform.json", 'r') as file:
            self.numeric_ids = json.load(file)["mobos_numeric_ids"]

        # Let's create the QBox Schedulers
        for (qb_name, dict_qrads) in dict_ids.items():
            site = self.site_from_qb_name(qb_name)
            qb = self.qbox_sched_name(qb_name, dict_qrads, site, self.bs, self, self.storage_controller)

            self.dict_qboxes[qb_name] = qb
            self.dict_sites[site].append(qb)
            for qr_name in dict_ids[qb_name].keys():
                self.dict_qrads[qr_name] = qb

            # Populate the mapping between batsim resource ids and the associated QarnotBoxSched
            for mobos_list in dict_qrads.values():
                for (batid, qm_name, _) in mobos_list:
                    self.dict_resources[batid] = qb
                    numeric_id = self.numeric_ids[qm_name]
                    # Just a guard to be sure.
                    assert batid == numeric_id, "{} and {} are different (types are {} {})".format(batid, numeric_id, type(batid), type(numeric_id))

        self.nb_qboxes = len(self.dict_qboxes)
        self.nb_computing_resources = len(self.dict_resources)


    def site_from_qb_name(self, qb_name):
        if qb_name.split('-')[1] == "2000":
            return "bordeaux"
        else:
            return "paris"


    def onRequestedCall(self):
        self.update_in_current_step = True#pass


    def onNoMoreJobsInWorkloads(self):
        pass
        '''self.logger.info("There is no more static jobs in the workload")
        self.logger.info("[{}] Info from QNode:".format(self.bs.time()))
        for qtask in self.qtasks_queue.values():
            qtask.print_infos(self.logger)'''

    def onNoMoreExternalEvent(self):
        pass
        '''self.logger.info("There is no more external events to occur")
        self.logger.info("[{}] Info from QNode:".format(self.bs.time()))
        for qtask in self.qtasks_queue.values():
            qtask.print_infos(self.logger)'''


    def onBeforeEvents(self):
        if self.bs.time() >= self.next_print:
            print(self.bs.time(), (math.floor(self.bs.time())/86400.0))
            self.next_print+=43200
        if self.bs.time() > self.max_simulation_time:
            self.logger.info("[{}] SYS EXIT".format(self.bs.time()))
            sys.exit(1)

        self.logger.info("\n")
        for qb in self.dict_qboxes.values():
            qb.onBeforeEvents()

        if self.bs.time() >= self.time_next_update:
            self.updateAllQBoxes()

            self.time_next_update = math.floor(self.bs.time()) + self.update_period

            if self.very_first_update: # First update at t=1, next updates every 30 seconds starting at t=30
                self.time_next_update -= 1
                self.very_first_update = False
                self.update_in_current_step = True # We will ask for the next wake up in onNoMoreEvents if the simulation is not finished yet

            self.do_dispatch = True # We will do a dispatch anyway
        else:
            self.do_dispatch = False # We may not have to dispatch, depending on the events in this message


    def onNoMoreEvents(self):
        if self.end_of_simulation_asked:
            return

        if (self.bs.time() >= 1.0) and self.do_dispatch:
            self.doDispatch()

        for qb in self.dict_qboxes.values():
            # Will do the frequency regulation for each QBox
            qb.onNoMoreEvents()

        # If the simulation is not finished and we need to ask Batsim for the next waking up
        if not self.end_of_simulation_asked and self.update_in_current_step:
            self.bs.wake_me_up_at(self.time_next_update)
            self.update_in_current_step = False


    def updateAllQBoxes(self):
        self.logger.info("[{}]- QNode calling update on QBoxes".format(self.bs.time()))
        # It's time to ask QBoxes to update and report their state
        self.lists_available_mobos = []
        for qb in self.dict_qboxes.values():
            # This will update the list of availabilities of the QMobos (and other QBox-related stuff)
            tup = qb.updateAndReportState()
            self.lists_available_mobos.append(tup)

    def onNotifyEventTargetTemperatureChanged(self, qrad_name, new_temperature):
        self.dict_qrads[qrad_name].onTargetTemperatureChanged(qrad_name, new_temperature)


    def onNotifyEventOutsideTemperatureChanged(self, machines, new_temperature):
        pass
        ''' This is not used by the qarnot schedulers
        for machine_id in machines:
            self.dict_resources[machine_id].onOutsideTemperatureChanged(new_temperature)
        '''

    def onNotifyEventMachineUnavailable(self, machines):
        for machine_id in machines:
            self.dict_resources[machine_id].onNotifyMachineUnavailable(machine_id)

    def onNotifyEventMachineAvailable(self, machines):
        for machine_id in machines:
            self.dict_resources[machine_id].onNotifyMachineAvailable(machine_id)

    def onMachinePStateChanged(self, nodeid, pstate):
        pass

    def onNotifyGenericEvent(self, event_data):
        if event_data["type"] == "stop_simulation":
            self.logger.info("[{}] Simulation STOP asked by an event".format(self.bs.time()))
            self.killOrRejectAllJobs()
            self.bs.notify_registration_finished()
            self.end_of_simulation_asked = True
        else:
            pass # TODO need to handle jobs already running somewhere here (with dynamic jobs?)

    def onJobSubmission(self, job, resubmit=False):
        qtask_id = job.id.split('_')[0]
        job.qtask_id = qtask_id

        # Retrieve or create the corresponding QTask
        if not qtask_id in self.qtasks_queue:
            assert resubmit == False, "QTask id not found during resubmission of an instance"
            
            list_datasets = self.bs.profiles[job.workload][job.profile]["datasets"]
            if list_datasets is None:
                list_datasets = []
            
            qtask = QTask(qtask_id, job.profile_dict["priority"], list_datasets)
            self.qtasks_queue[qtask_id] = qtask
        else:
            qtask = self.qtasks_queue[qtask_id]

        qtask.instance_submitted(job, resubmit)

        self.do_dispatch = True
        #TODO maybe we'll need to disable this dispatch, same reason as when a job completes


    def resubmitJob(self, job):
        if job.metadata is None:
            metadata = {"parent_job": job.id, "nb_resubmit": 1}
        else:
            metadata = deepcopy(job.metadata)
            if "nb_resubmit" not in metadata:
                metadata["nb_resubmit"] = 1
            else:
                metadata["nb_resubmit"] = metadata["nb_resubmit"] + 1
            if "parent_job" not in metadata:
                metadata["parent_job"] = job.id

        # Keep the current workload and add a resubmit number
        splitted_id = job.id.split(self.bs.ATTEMPT_JOB_SEPARATOR)
        if len(splitted_id) == 1:
            new_job_id = deepcopy(job.id)
        else:
            # This job as already an attempt number
            new_job_id = splitted_id[0]
            assert splitted_id[1] == str(metadata["nb_resubmit"] - 1)

        # Since ACK of dynamic jobs registration is disabled, submit it manually
        new_job_id = new_job_id + Batsim.ATTEMPT_JOB_SEPARATOR + str(metadata["nb_resubmit"])
        new_job = self.bs.register_job(new_job_id, job.requested_resources, job.requested_time, job.profile)
        new_job.metadata = metadata

        self.logger.info("[{}] QNode resubmitting {} with new id {}".format(self.bs.time(), job, new_job_id))
        self.onJobSubmission(new_job, resubmit=True)


    def onQBoxRejectedInstances(self, instances, qb_name):
        #Should not happen a lot of times
        self.nb_rejected_jobs_by_qboxes += len(instances)
        qtask = self.qtasks_queue[instances[0].qtask_id]
        for job in instances:
            self.jobs_mapping.pop(job.id)
            qtask.instance_rejected(job)


    def onJobCompletion(self, job):
        if job.workload == "dyn-burn":
            assert job.job_state != Job.State.COMPLETED_SUCCESSFULLY, "CPU burn job on machine {} finished, this should never happen".format(str(job.allocation))
            # If the job was killed, don't do anything

        elif job.workload == "dyn-staging":
            if job.job_state == Job.State.COMPLETED_SUCCESSFULLY:
                self.storage_controller.onDataStagingCompletion(job)
            elif job.job_state == Job.State.COMPLETED_KILLED:
                self.storage_controller.onDataStagingKilled(job)
            else:
                assert False, "Data staging job {} reached the state {}, this should not happen.".format(job.id, job.job_state)

        else:
            # This should either be a job from a static workflow or from "dyn-resubmit"
            # (or another name of worklow for re-submitted instances that were previously preempted)
            qtask = self.qtasks_queue[job.qtask_id]
            if job.job_state == Job.State.COMPLETED_KILLED:
                qtask.instance_killed()
                qb = self.jobs_mapping.pop(job.id)
                qb.onJobKilled(job)

                if self.end_of_simulation_asked == False:
                    #This should be an instance preempted by an instance of higher priority
                    self.nb_preempted_jobs += 1
                    self.resubmitJob(job)

            elif job.job_state == Job.State.COMPLETED_SUCCESSFULLY:
                qtask.instance_finished()
                qb = self.jobs_mapping.pop(job.id)

                #Check if direct dispatch is possible
                if self.direct_dispatch_enabled:
                    ''' DIRECT DISPATCH '''
                    direct_job = self.tryDirectDispatch(qtask, qb)
                    qb.onJobCompletion(job, direct_job)
                else:
                    qb.onJobCompletion(job)
                    # A slot should be available, do a general dispatch
                    # TODO actually no, because update of available slots is not done between now and the do_dispatch
                    # TODO If we run simulations with update_period of more than 30 seconds we need to update the slots here
                    #            when an instance completes
                    self.do_dispatch = True

                    #Check if the QTask is complete
                    if qtask.is_complete():
                        self.logger.info("[{}]    All instances of QTask {} have terminated, removing it from the queue".format(self.bs.time(), qtask.id))
                        del self.qtasks_queue[qtask.id]
            else:
                # "Regular" instances are not supposed to have a walltime nor fail
                assert False, "Job {} reached the state {}, this should not happen.".format(job.id, job.job_state)
        #End if/else on job.workload
    #End onJobCompletion

    def tryDirectDispatch(self, qtask, qb):
        if len(qtask.waiting_instances) > 0 and not self.existsHigherPriority(qtask.priority):
            #This Qtask still has instances to dispatch and it has the highest priority in the queue
            direct_job = qtask.instance_poped_and_dispatched()
            self.jobs_mapping[direct_job.id] = qb
            self.logger.info("[{}]- QNode asked direct dispatch of {} on QBox {}".format(self.bs.time(),direct_job.id, qb.name))
            return direct_job
        else:
            return -1 # No job is directly dispatched

    def onJobsKilled(self, job):
        pass
        #TODO pass?


    def killOrRejectAllJobs(self):
        self.logger.info("Killing all running jobs and rejecting all waiting ones.")
        to_reject = []
        to_kill = []
        for qb in self.dict_qboxes.values():
            (qb_reject, qb_kill) = qb.killOrRejectAllJobs()
            to_reject.extend(qb_reject)
            to_kill.extend(qb_kill)

        for qtask in self.qtasks_queue.values():
            qtask.print_infos(self.logger)
            if len(qtask.waiting_instances) > 0:
                to_reject.extend(qtask.waiting_instances)

        to_kill.extend(self.storage_controller.onKillAllStagingJobs())

        if len(to_reject) > 0:
            self.logger.info("Rejecting {} jobs".format(len(to_reject)))
            self.bs.reject_jobs(to_reject)

        if len(to_kill) > 0:
            self.logger.info("Killing {} jobs".format(len(to_kill)))
            self.bs.kill_jobs(to_kill)


        self.logger.info("Now Batsim should stop the simulation on next message (or after next REQUESTED_CALL)")


    def sortAvailableMobos(self, priority):
        #self.lists_available_mobos.sort(key=lambda tup:tup[0])
        if priority == "bkgd":
            self.lists_available_mobos.sort(key=lambda tup:(tup[0],tup[1]))
        elif priority == "low":
            self.lists_available_mobos.sort(key=lambda tup:(tup[0],tup[2]))
        elif priority == "high":
            self.lists_available_mobos.sort(key=lambda tup:(tup[0],tup[3]))


    def addJobsToMapping(self, jobs, qb):
        for job in jobs:
            self.jobs_mapping[job.id] = qb


    def existsHigherPriority(self, priority):
        '''
        Returns whether there is a QTask of strictly higher priority than the one given in parameter
        have still an instance waiting to be dispatched
        This is used to know whether quickDispatch is possible upon completion of an instance
        '''
        for qtask in self.qtasks_queue.values():
            if (qtask.priority > priority) and (len(qtask.waiting_instances) > 0):
                return True
        return False
