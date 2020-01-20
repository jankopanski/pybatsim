from batsim.batsim import BatsimScheduler, Batsim, Job
from StorageController import StorageController
from qarnotUtils import *
from qarnotBoxSched import QarnotBoxSched

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
This is the scheduler instanciated by Pybatsim for a simulation of the Qarnot platform and has two roles:
- It does the interface between Batsim/Pybatsim API and the QNode/QBox schedulers (manager)
- It holds the implementation of the Qarnot QNode scheduler (dispatcher)


Every 30 seconds from t=0 the QNode scheduler will be woken up by Batsim.
At this time, the QBox sched will be asked to update their state (cf function updateAndReportState of the QBox scheduler)

Every time the QNode sched will be woken up, a dispatch of the tasks in the queue will be performed after each event
in the message from Batsim was handled.


Notion of priority for tasks:
There are 3 major groups: Bkgd, Low and High
Each task has a certain numerical value of priority.

A task cannot preempt a task in the same priority group (even if its numerical priority is smaller)
but can preempt tasks from a lower priority group.


A job refers to a Batsim job (see batsim.batsim.Job)
A task or QTask refers to a Qarnot QTask (see qarnotUtils.QTask)

A QTask can either be a set of instances, or a cluster.
If the number of requested resources of a Batsim Job is > 1, then it's a cluster.
For a cluster, its execution time is fixed no matter the speed of the machines executing it.
When a cluster is started, it should execute until its end, and should be killed when the time is reached.
TODO What if the cluster is killed before its end due to higher priority or too hot rad?

'''

class QarnotNodeSched(BatsimScheduler):
    def __init__(self, options):
        super().__init__(options)

        self.logger.setLevel(logging.DEBUG)

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

        if "handle_availability" in options:
            self.forward_availability_events = True if options["handle_availability"] == 1 else False
        else:
            self.forward_availability_events = False

        # For the manager
        self.dict_qboxes = {}        # Maps the QBox id to the QarnotBoxSched object
        self.dict_qrads = {}         # Maps the QRad name to the QarnotBoxSched object
        self.dict_resources = {} # Maps the Batsim resource id to the QarnotBoxSched object
        self.dict_procsets = defaultdict(ProcSet) # Maps the QBox id to a ProcSet of its QMobos
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
        self.nb_killed_jobs = 0
        self.nb_received_qtasks = 0
        self.nb_received_instances = 0
        self.nb_received_clusters = 0

        #self.max_simulation_time = 87100 # 1 day
        self.max_simulation_time = 1211000 # 2 weeks TODO remove this guard?
        self.end_of_simulation_asked = False
        self.next_print = 43200


    def onSimulationBegins(self):
        assert self.bs.dynamic_job_registration_enabled, "Registration of dynamic jobs must be enabled for this scheduler to work"
        assert self.bs.allow_storage_sharing, "Storage sharing must be enabled for this scheduler to work"
        assert self.bs.ack_of_dynamic_jobs == False, "Acknowledgment of dynamic jobs must be disabled for this scheduler to work"

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

        (nb_transfers_zero, nb_transfers_real, total_transferred_from_CEPH) = self.storage_controller.onSimulationEnds()

        print("Number of received QTasks:", self.nb_received_qtasks)
        print("number of received instances:", self.nb_received_instances)
        print("Number of received clusters:", self.nb_received_clusters)
        print("Number of rejected instances by QBoxes during dispatch:", self.nb_rejected_jobs_by_qboxes)
        print("Number of burn jobs created:", self.next_burn_job_id)
        print("Number of staging jobs created:", self.storage_controller._next_staging_job_id)
        print("Number of killed jobs:", self.nb_killed_jobs)
        print("Update_period was:", self.update_period)

        if self.output_filename != None:
            self.write_output_to_file(nb_transfers_zero, nb_transfers_real, total_transferred_from_CEPH)

    def write_output_to_file(self, nb_transfers_zero, nb_transfers_real, total_transferred_from_CEPH):
        print("Writing outputs to", self.output_filename)
        if not os.path.exists(os.path.dirname(self.output_filename)):
            os.makedirs(os.path.dirname(self.output_filename))
        with open(self.output_filename, 'w', newline='') as csvfile:
            fieldnames = ['update_period', 'nb_received_qtasks', 'nb_received_instances', 'nb_received_clusters',
                          'nb_rejected_instances_during_dispatch', 'nb_burn_jobs_created',
                          'nb_staging_jobs_created', 'nb_killed_jobs',
                          'nb_transfers_zero', 'nb_transfers_real', 'total_transferred_GB']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({
                'update_period': self.update_period,
                'nb_received_qtasks': self.nb_received_qtasks,
                'nb_received_instances': self.nb_received_instances,
                'nb_received_clusters': self.nb_received_clusters,
                'nb_rejected_instances_during_dispatch': self.nb_rejected_jobs_by_qboxes,
                'nb_burn_jobs_created': self.next_burn_job_id,
                'nb_staging_jobs_created': self.storage_controller._next_staging_job_id,
                'nb_killed_jobs': self.nb_killed_jobs,
                'nb_transfers_zero' : nb_transfers_zero,
                'nb_transfers_real' : nb_transfers_real,
                'total_transferred_GB' : (total_transferred_from_CEPH / 1e9),

                })


    def initQBoxesAndStorageController(self):
        # Let's create the StorageController
        self.storage_controller = self.storage_controller_name(self.bs.machines["storage"], self.bs, self, self.options)

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
                    self.dict_procsets[qb_name].insert(batid)

                    # Just a guard to be sure.
                    numeric_id = self.numeric_ids[qm_name]
                    assert batid == numeric_id, "{} and {} are different (types are {} {})".format(batid, numeric_id, type(batid), type(numeric_id))

        self.nb_qboxes = len(self.dict_qboxes)
        self.nb_computing_resources = len(self.dict_resources)


    def site_from_qb_name(self, qb_name):
        if qb_name.split('-')[1] == "2000":
            return "bordeaux"
        elif qb_name.split('-')[1] == "4000":
            return "reau"
        else:
            return "paris"


    def onRequestedCall(self):
        self.update_in_current_step = True#pass


    def onNoMoreJobsInWorkloads(self):
        pass

    def onNoMoreExternalEvent(self):
        pass


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


    def onNotifyEventMachineUnavailable(self, machines):
        if not self.forward_availability_events:
            return

        for qb_name, procset_mobos in self.dict_procsets.items():
            p = (machines & procset_mobos) # Computes the intersection
            self.dict_qboxes[qb_name].onNotifyMachineUnavailable(p)

    def onNotifyEventMachineAvailable(self, machines):
        if not self.forward_availability_events:
            return

        for qb_name, procset_mobos in self.dict_procsets.items():
            p = (machines & procset_mobos) # Computes the intersection
            self.dict_qboxes[qb_name].onNotifyMachineAvailable(p)

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

    def killOrRejectAllJobs(self):
        self.logger.info(f"[{self.bs.time()}] Killing all running jobs and rejecting all waiting ones.")

        to_reject = []
        to_kill = []
        for qb in self.dict_qboxes.values():
            (qb_reject, qb_kill) = qb.killOrRejectAllJobs()
            to_reject.extend(qb_reject)
            to_kill.extend(qb_kill)

        for qtask in self.qtasks_queue.values():
            #qtask.print_infos(self.logger)
            if len(qtask.waiting_instances) > 0:
                to_reject.extend(qtask.waiting_instances)

        to_kill.extend(self.storage_controller.onKillAllStagingJobs())

        if len(to_reject) > 0:
            self.logger.info(f"[{self.bs.time()}] Rejecting {len(to_reject)} jobs")
            self.bs.reject_jobs(to_reject)

        if len(to_kill) > 0:
            self.logger.info(f"[{self.bs.time()}] Killing {len(to_kill)} jobs")
            self.bs.kill_jobs(to_kill)

        self.logger.info(f"[{self.bs.time()}] Now Batsim should stop the simulation on next message (or after next REQUESTED_CALL)")



    def onJobSubmission(self, job, resubmit=False):
        qtask_id = (job.id.split('_')[0]).split('#')[0] # To remove the instance number and resubmission number, if any
        job.qtask_id = qtask_id

        # Retrieve or create the corresponding QTask
        if not qtask_id in self.qtasks_queue:
            assert resubmit == False, f"QTask id {qtask_id} not found during resubmission of an instance"
            
            list_datasets = self.bs.profiles[job.workload][job.profile]["datasets"]
            if list_datasets is None:
                list_datasets = []
            
            user_id = job.profile_dict["user"]

            qtask = QTask(qtask_id, job.profile_dict["priority"], list_datasets, user_id)
            self.qtasks_queue[qtask_id] = qtask
            self.nb_received_qtasks += 1

            self.logger.debug(f"== Adding QTask {qtask_id} in the dict")

            # Check whether it is a cluster
            if job.requested_resources > 1:
                self.nb_received_clusters += 1
                qtask.cluster_task = True
                qtask.requested_resources = job.requested_resources
                qtask.execution_time = job.requested_time # This should be equal to real_finish_time - real_start_time, or less if this is a resubmission of the cluster task
                self.logger.info(f'New cluster {job.id} submitted with execution time {qtask.execution_time}')

            else:
                self.nb_received_instances += 1

        else: # The QTask already exists
            if (job.requested_resources > 1) and (resubmit == False):
                # Two clusters should not have the same guid
                assert False, f"New cluster {qtask_id} already present in the list of Qtasks, this should not happen."
            else:
                qtask = self.qtasks_queue[qtask_id]
                self.nb_received_instances += 1

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
        splitted_id = job.id.split(Batsim.ATTEMPT_JOB_SEPARATOR)
        if len(splitted_id) == 1:
            new_job_id = deepcopy(job.id)
            new_profile_id = deepcopy(job.profile)
        else:
            assert splitted_id[1] == str(metadata["nb_resubmit"] - 1)

            # This job has already an attempt number
            new_job_id = splitted_id[0]
            new_profile_id = job.profile.split(Batsim.ATTEMPT_JOB_SEPARATOR)[0]

        new_job_id = new_job_id + Batsim.ATTEMPT_JOB_SEPARATOR + str(metadata["nb_resubmit"])
        new_profile_id = new_profile_id + Batsim.ATTEMPT_JOB_SEPARATOR + str(metadata["nb_resubmit"])

        # Since ACK of dynamic jobs registration is disabled, submit it manually
        # Submit a copy of the profile first
        #self.logger.info(f"\n[{self.bs.time()}] Profiles:\n{self.bs.profiles[job.workload].keys()}")
        copy_profile = self.bs.profiles[job.workload][job.profile]
        self.bs.register_profile(job.workload, new_profile_id, copy_profile)
        new_job = self.bs.register_job(new_job_id, job.requested_resources, job.requested_time, new_profile_id)
        new_job.metadata = metadata

        if job.requested_resources > 1:
            # It is a cluster, update its walltime
            time_running = self.bs.time () - job.start_time
            self.logger.debug("1===== {} {}".format(self.bs.jobs[new_job_id].requested_time, new_job.requested_time))
            new_job.requested_time = job.requested_time - time_running
            self.logger.debug("2===== {} {}}".format(self.bs.jobs[new_job_id].requested_time, new_job.requested_time))
            self.logger.debug(f"Resubmitted cluster {job.id} with new walltime {new_job.requested_time} instead of {job.requested_time} after running from {job.start_time} to {self.bs.time()}")

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

                self.nb_killed_jobs += 1
                if self.end_of_simulation_asked == False:
                    #This should be an instance killed because the rad was too hot or preempted by an instance of higher priority
                    self.resubmitJob(job)

            elif job.job_state == Job.State.COMPLETED_SUCCESSFULLY:
                qtask.instance_finished()
                qb = self.jobs_mapping.pop(job.id)

                #Check if direct dispatch is possible
                if self.direct_dispatch_enabled and not qtask.is_cluster(): # Because cluster QTasks do not have multiple instances
                    ''' DIRECT DISPATCH '''
                    direct_job = self.tryDirectDispatch(qtask, qb)
                    self.logger.debug(f"Sending job completion of {job.id} to {qb.name} with direct job {direct_job}")
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
                        self.logger.debug(f"== Removing {qtask.id} from the dict")
                        del self.qtasks_queue[qtask.id]

            elif job.job_state == Job.State.COMPLETED_WALLTIME_REACHED:
                # Only cluster tasks have walltimes
                assert qtask.is_cluster(), f"Job {job.id} reached its walltime but is not a cluster task."

                # If the walltime is reached, that means that the cluster has completed
                qtask.instance_finished()
                qb = self.jobs_mapping.pop(job.id)
                qb.onJobCompletion(job)
                self.do_dispatch = True

                assert qtask.is_complete(), f"Cluster QTask {qtask.id} is not complete after its walltime was reached."
                self.logger.info(f"[{self.bs.time()}]    Cluster task {qtask.id} has completed, removing it from the queue")
                self.logger.debug(f"== Removing {qtask.id} from the dict")
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
            self.logger.debug(f"Adding {job.id} to mapping on qb {qb.name}")
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


    def doDispatch(self):
        '''For the dispatch of a task:
        Select the task of highest priority that have the smallest number of running instances.
        Then to choose the QBoxes/Mobos where to send it:
        - First send as much instances as possible on mobos available for bkgd
            (the ones that are running bkgd tasks, because they have the most "coolness reserve"
            and will likely run faster and longer than idle cpus)

        - Second on mobos available for low
        - Third on mobos available for high

        The QBoxes where the instances are sent are sorted by increasing number of available mobos
        (so keep large blocks of mobos available for cluster tasks)
        Always send as much instances as there are available mobos in a QBox
        '''

        if len(self.qtasks_queue) == 0:
            self.logger.info("[{}]- QNode has nothing to dispatch".format(self.bs.time()))
            return

        # Sort the jobs by decreasing priority (hence the '-' sign) and then by increasing number of running instances
        self.logger.info("[{}]- QNode starting doDispatch".format(self.bs.time()))
        for qtask in sorted(self.qtasks_queue.values(),key=lambda qtask:(-qtask.priority, qtask.nb_dispatched_instances)):
            nb_instances_left = len(qtask.waiting_instances)
            if nb_instances_left > 0:
                self.logger.debug("[{}]- QNode trying to dispatch {} of priority {} having {} waiting and {} dispatched instances".format(self.bs.time(),qtask.id, qtask.priority, len(qtask.waiting_instances), qtask.nb_dispatched_instances))

                if qtask.is_cluster():
                    assert len(qtask.waiting_instances) == 1, f"Trying to dispatch a cluster task {qtask.id} having more than one waiting instance, aborting." # Just to be sure

                    # A cluster QTask must be dispatched entirely on the same QBox
                    for tup in self.lists_available_mobos:
                        nb_available_slots = tup[1] # The BKGD ones
                        if qtask.priority_group > PriorityGroup.BKGD:
                            nb_available_slots += tup[2] # The LOW ones
                            if qtask.priority_group > PriorityGroup.LOW:
                                nb_available_slots += tup[3] # The HIGH ones

                        if nb_available_slots >= qtask.requested_resources:
                            jobs = qtask.waiting_instances.copy()
                            qtask.instances_dispatched(jobs)
                            qb = self.dict_qboxes[tup[0]]
                            self.addJobsToMapping(jobs, qb)
                            self.logger.debug("[{}]- QNode now dispatched cluster task {} requiring {} on {} having {} available slots.".format(self.bs.time(),qtask.id, qtask.requested_resources, qb.name, nb_available_slots))
                            qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id, True) # The True argument means that this dispatched instance is a cluster

                            # Remove the used number of slots from the tup, starting from BKGD
                            slots_to_remove = qtask.requested_resources
                            if slots_to_remove <= tup[1]: # Take from BKGD slots
                                tup[1] -= slots_to_remove
                            else:
                                slots_to_remove -= tup[1]
                                tup[1] = 0
                                if slots_to_remove <= tup[2]: # Take from LOW slots
                                    tup[2] -= slots_to_remove
                                else:
                                    slots_to_remove -= tup[2]
                                    tup[2] = 0

                                    assert qtask.priority_group > PriorityGroup.LOW, f"Cluster {qtask.id} of priority <= LOW was dispatched on {qb.name} but there were not enough slots for it..." # This should never happen
                                    assert slots_to_remove <= tup[3], f"Cluster {qtask.id} was dispatched on {qb.name} but there were not enough slots for it..." # This should never happen
                                    tup[3] -= slots_to_remove
                            # End updating slots of tup

                            break # Get out of the for loop
                    #End for tup

                else: # This is a regular QTask with instances
                    #instances_to_dispatch = defaultdict(list)
                    # Dispatch as many instances as possible on mobos available for bkgd, no matter the priority of the qtask
                    self.sortAvailableMobos("bkgd")
                    for tup in self.lists_available_mobos:
                        qb = self.dict_qboxes[tup[0]]
                        nb_slots = tup[1]
                        if nb_slots >= nb_instances_left:
                            # There are more available slots than instances, gotta dispatch'em all!
                            jobs = qtask.waiting_instances.copy()
                            self.addJobsToMapping(jobs, qb)                     # Add the Jobs to the internal mapping
                            qtask.instances_dispatched(jobs)                    # Update the QTask
                            qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id) # Dispatch the instances
                            #instances_to_dispatch.extend(jobs)
                            tup[1] -= nb_instances_left                             # Update the number of slots in the list
                            nb_instances_left = 0
                            # No more instances are waiting, stop the dispatch for this qtask
                            break
                        elif nb_slots > 0: # 0 < nb_slots < nb_instances_left
                            # Schedule instances for all slots of this QBox
                            jobs = qtask.waiting_instances[0:nb_slots]
                            self.addJobsToMapping(jobs, qb)
                            qtask.instances_dispatched(jobs)
                            qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id)
                            #instances_to_dispatch.extend(jobs)
                            tup[1] = 0
                            nb_instances_left -= nb_slots
                    #End for tup in bkgd slots

                    if (nb_instances_left > 0) and (qtask.priority_group > PriorityGroup.BKGD):
                        # There are more instances to dispatch and the qtask is either low or high priority
                        self.sortAvailableMobos("low")
                        for tup in self.lists_available_mobos:
                            qb = self.dict_qboxes[tup[0]]
                            nb_slots = tup[2]
                            if nb_slots >= nb_instances_left:
                                # There are more available slots than instances, gotta dispatch'em all!
                                jobs = qtask.waiting_instances.copy()
                                self.addJobsToMapping(jobs, qb)
                                qtask.instances_dispatched(jobs)
                                qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id)
                                #instances_to_dispatch.extend(jobs)
                                tup[2] -= nb_instances_left
                                nb_instances_left = 0
                                # No more instances are waiting, stop the dispatch for this qtask
                                break
                            elif nb_slots > 0: # 0 < nb_slots < nb_instances_left
                                # Schedule instances for all slots of this QBox
                                jobs = qtask.waiting_instances[0:nb_slots]
                                self.addJobsToMapping(jobs, qb)
                                qtask.instances_dispatched(jobs)
                                qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id)
                                #instances_to_dispatch.extend(jobs)
                                tup[2] = 0
                                nb_instances_left -= nb_slots
                        #End for tup low slots

                        if (nb_instances_left > 0) and (qtask.priority_group > PriorityGroup.LOW):
                            # There are more instances to dispatch and the qtask is high priority
                            self.sortAvailableMobos("high")
                            for tup in self.lists_available_mobos:
                                qb = self.dict_qboxes[tup[0]]
                                nb_slots = tup[3]
                                if nb_slots >= nb_instances_left:
                                    # There are more available slots than wild instances, gotta catch'em all!
                                    jobs = qtask.waiting_instances.copy()
                                    self.addJobsToMapping(jobs, qb)
                                    qtask.instances_dispatched(jobs)
                                    qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id)
                                    #instances_to_dispatch.extend(jobs)
                                    tup[3] -= nb_instances_left
                                    nb_instances_left = 0
                                    # No more instances are waiting, stop the dispatch for this qtask
                                    break
                                elif nb_slots > 0: # 0 < nb_slots < nb_instances_left
                                    # Schedule instances for all slots of this QBox
                                    jobs = qtask.waiting_instances[0:nb_slots]
                                    self.addJobsToMapping(jobs, qb)
                                    qtask.instances_dispatched(jobs)
                                    qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id)
                                    #instances_to_dispatch.extend(jobs)
                                    tup[3] = 0
                                    nb_instances_left -= nb_slots
                            #End for tup high slots
                        #End if high priority and nb_instances_left > 0
                    #End if low/high priority and nb_instances_left > 0
                    #qb.onDispatchedInstance(instances_to_dispatch, qtask.priority_group, qtask.id)
                    self.logger.debug("[{}]- QNode now dispatched a total of {} instances of {}, {} are still waiting.".format(self.bs.time(),qtask.nb_dispatched_instances, qtask.id, len(qtask.waiting_instances)))
                #End if qtask.is_cluster()
            #End if nb_instances_left > 0
        #End for qtasks in queue
        self.logger.info("[{}]- QNode end of doDispatch".format(self.bs.time()))
    #End of doDispatch function
