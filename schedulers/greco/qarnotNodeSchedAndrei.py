from batsim.batsim import BatsimScheduler, Batsim, Job
from StorageController import StorageController
from qarnotUtilsAndrei import *
from qarnotBoxSched import QarnotBoxSched

from procset import ProcSet
from collections import defaultdict

import math
import logging
import sys
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
'''

class QarnotNodeSchedAndrei(BatsimScheduler):
        def __init__(self, options):
                super().__init__(options)

                #self.logger.setLevel(logging.CRITICAL)

                # For the manager
                self.dict_qboxes = {}        # Maps the QBox id to the QarnotBoxSched object
                self.dict_resources = {} # Maps the Batsim resource id to the QarnotBoxSched object
                self.dict_qrads = {}         # Maps the QRad name to the QarnotBoxSched object
                self.dict_sites = defaultdict(list) # Maps the site name ("paris" or "bordeaux" for now) to a list of QarnotBoxSched object

                # Dispatcher
                self.qtasks_queue = {}     # Maps the QTask id to the QTask object that is waiting to be scheduled
                self.jobs_mapping = {}     # Maps the batsim job id to the QBox Object where it has been sent to
                
                self.lists_available_mobos = [] # List of [qbox_name, slots bkgd, slots low, slots high]
                                                                                # This list is updated every 30 seconds from the reports of the QBoxes
                

                #NOT USED AT THE MOMENT:
                #self.qboxes_free_disk_space = {} # Maps the QBox id to the free disk space (in GB)
                #self.qboes_queued_upload_size = {} # Maps the QBox id to the queued upload size (in GB)


                #self.update_period = 30 # The scheduler will be woken up by Batsim every 30 seconds
                self.update_period = 600 # TODO every 10 minutes for testing
                self.time_next_update = 1.0 # The next time the scheduler should be woken up
                self.ask_next_update = False # Whether to ask for next update in onNoMoreEvents function
                self.next_update_asked = False     # Whether the 'call_me_later' has been sent or not

                self.very_first_update = True

                self.nb_rejected_jobs_by_qboxes = 0 # Just to keep track of the count

                #self.max_simulation_time = 87100 # 1 day
                self.max_simulation_time = 1211000 # 1 week TODO remove this guard?
                self.end_of_simulation_asked = False


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

            self.logger.info("Number of rejected instances by QBoxes during dispatch: %s", self.nb_rejected_jobs_by_qboxes)


        def initQBoxesAndStorageController(self):
            # Let's create the StorageController
            self.storage_controller = StorageController(self.bs.machines["storage"], self.bs, self)


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

            # Let's create the QBox Schedulers
            for (qb_name, dict_qrads) in dict_ids.items():

                site = self.site_from_qb_name(qb_name)
                qb = QarnotBoxSched(qb_name, dict_qrads, site, self.bs, self, self.storage_controller)

                self.dict_qboxes[qb_name] = qb
                self.dict_sites[site].append(qb)
                for qr_name in dict_ids[qb_name].keys():
                    self.dict_qrads[qr_name] = qb

                # Populate the mapping between batsim resource ids and the associated QarnotBoxSched
                for mobos_list in dict_qrads.values():
                    for (batid, _, _) in mobos_list:
                        self.dict_resources[batid] = qb

            self.nb_qboxes = len(self.dict_qboxes)
            self.nb_computing_resources = len(self.dict_resources)

        def site_from_qb_name(self, qb_name):
            if qb_name.split('-')[1] == "2000":
                return "bordeaux"
            else:
                return "paris"


        def onRequestedCall(self):
            self.ask_next_update = True#pass


        def onNoMoreJobsInWorkloads(self):
            self.logger.info("There is no more static jobs in the workload")
            self.logger.info("[{}] Info from QNode:".format(self.bs.time()))
            for qtask in self.qtasks_queue.values():
                qtask.print_infos(self.logger)

        def onNoMoreExternalEvent(self):
            self.logger.info("There is no more external evetns to occur")
            self.logger.info("[{}] Info from QNode:".format(self.bs.time()))
            for qtask in self.qtasks_queue.values():
                qtask.print_infos(self.logger)


        def onBeforeEvents(self):
            if self.bs.time() > self.max_simulation_time:
                self.logger.info(str(self.bs.time()), "SYS EXIT")
                sys.exit(1)

            self.logger.info("\n")
            
            for qb in self.dict_qboxes.values():
                qb.onBeforeEvents()

            if self.bs.time() >= self.time_next_update:
                self.logger.info("[{}]- QNode calling update on QBoxes".format(self.bs.time()))
                # It's time to ask QBoxes to update and report their state
                self.lists_available_mobos = []
                for qb in self.dict_qboxes.values():
                    # This will update the list of availabilities of the QMobos (and other QBox-related stuff)
                    tup = qb.updateAndReportState()
                    self.lists_available_mobos.append(tup)
                self.time_next_update = math.floor(self.bs.time()) + self.update_period


                if self.very_first_update: # First update at t=1, next updates every 30 seconds starting at t=30
                    self.time_next_update -= 1
                    self.very_first_update = False

                    self.ask_next_update = True # We will ask for the next wake up in onNoMoreEvents if the simulation is not finished yet

                self.do_dispatch = True # We will do a dispatch anyway
            else:
                self.do_dispatch = False # We may not have to dispatch, depending on the events in this message


        def onNoMoreEvents(self):
            if self.end_of_simulation_asked:
                return

            if (self.bs.time() >= 1.0) and self.do_dispatch:
                self.doDispatch()

            for qb in self.dict_qboxes.values():
                qb.onNoMoreEvents()

            # If the simulation is not finished and we need to ask Batsim for the next waking up
            self.checkNoMoreInstances()
            self.checkSimulationFinished()
            if not self.end_of_simulation_asked and self.ask_next_update:
                self.bs.wake_me_up_at(self.time_next_update)
                self.ask_next_update = False


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


        def onNotifyGenericEvent(self, event_data):
            if event_data["type"] == "stop_simulation":
                self.logger.info("[{}] Simulation STOP asked by an event".format(self.bs.time()))
                self.killOrRejectAllJobs()
                self.bs.notify_registration_finished()
                self.end_of_simulation_asked = True
                self.logger.info("Now Batsim should stop the simulation")
            else:
                pass # TODO need to handle jobs already running somewhere here (with dynamic jobs?)

        def onJobSubmission(self, job):
            qtask_id = job.id.split('_')[0]
            qtask_profile = job.profile
            job.qtask_id = qtask_id

            # Retrieve or create the corresponding QTask
            if not qtask_id in self.qtasks_queue:
                qtask = QTask(qtask_id, qtask_profile ,job.profile_dict["priority"])
                self.qtasks_queue[qtask_id] = qtask
            else:
                qtask = self.qtasks_queue[qtask_id]

            qtask.instance_submitted(job)
            self.do_dispatch = True
            #TODO maybe we'll need to disable this dispatch, same reason as when a job completes


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
                self.logger.info("[{}] CPU burn job on machine {} was just killed".format(self.bs.time(), job.allocation))
                # TODO maybe we'll need to forward it to the QBox, but I don't think so

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
                    # This should be an instance preempted by an instance of higher priority
                    qtask.instance_killed()
                    #TODO need to re-submit to Batsim a new instance and add it to the QTask.

                    qb = self.jobs_mapping.pop(job.id)
                    qb.onJobKilled(job)

                elif job.job_state == Job.State.COMPLETED_SUCCESSFULLY:
                    qtask.instance_finished()
                    qb = self.jobs_mapping.pop(job.id)

                    #Check if direct dispatch is possible
                    if len(qtask.waiting_instances) > 0 and not self.existsHigherPriority(qtask.priority):
                        ''' DIRECT DISPATCH '''
                        #This Qtask still has instances to dispatch and it has the highest priority in the queue
                        direct_job = qtask.instance_poped_and_dispatched()
                        self.jobs_mapping[direct_job.id] = qb
                        self.logger.info("[{}]- QNode asked direct dispatch of {} on QBox {}".format(self.bs.time(),direct_job.id, qb.name))
                        qb.onJobCompletion(job, direct_job)

                    else:
                        qb.onJobCompletion(job)
                        # A slot should be available, do a general dispatch
                        # TODO well actually that's not true since the slots are not updated...
                        #self.do_dispatch = True

                        #Check if the QTask is complete
                        if qtask.is_complete():
                            self.logger.info("[{}]    All instances of QTask {} have terminated, removing it from the queue".format(self.bs.time(), qtask.id))
                            del self.qtasks_queue[qtask.id]

                else:
                    # "Regular" instances are not supposed to have a walltime nor fail
                    assert False, "Job {} reached the state {}, this should not happen.".format(job.id, job.job_state)
            #End if/else on job.workload
        #End onJobCompletion

        def onJobsKilled(self, job):
            pass
            #TODO pass?

        def checkNoMoreInstances(self):
            if (len(self.qtasks_queue) == 0) and (len(self.jobs_mapping) == 0) and self.bs.no_more_static_jobs:
                self.logger.info("[{}] All static jobs seems to have finished. We could stop the simulation right now!".format(self.bs.time()))
                return True
            else:
                return False

        def checkSimulationFinished(self):
            # TODO This is a guard to avoid infinite loops.
            if self.bs.no_more_static_jobs and self.bs.no_more_external_events:
                self.logger.info("The simulation seems to be finished (no more static jobs or external events)."
                    " Killing or rejecting all remaining jobs.")
                self.end_of_simulation_asked = True
                self.killOrRejectAllJobs() # For Batsim to finish the simulation
                self.bs.notify_registration_finished()
                return True
            else:
                return False

        def killOrRejectAllJobs(self):
            to_reject = []
            for qtask in self.qtasks_queue.values():
                qtask.print_infos(self.logger)
                if len(qtask.waiting_instances) > 0:
                    to_reject.extend(qtask.waiting_instances)

            to_reject.extend(self.jobs_mapping.values())
            if len(to_reject) > 0:
                self.bs.reject_jobs(to_reject)

            self.logger.info("Now Batsim should stop the simulation on next message (or after next REQUESTED_CALL)")

        def sortAvailableMobos(self, priority):
            if priority == "bkgd":
                self.lists_available_mobos.sort(key=lambda nb:nb[1])
            elif priority == "low":
                self.lists_available_mobos.sort(key=lambda nb:nb[2])
            elif priority == "high":
                self.lists_available_mobos.sort(key=lambda nb:nb[3])


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

        def list_qboxes_with_dataset(self, qtask):
            ''' Lists all QBoxes that have the required list of datasets from the job 

            Could happen:
            - Required Data Set == NULL => qboxes_list empty
            - Required Data Set != NULL => qboxes_list empty or Not
            '''

            required_datasets = {} # To get the list of datasets requireds by the job
            qboxes_list = []
            profile = self.bs.profiles[qtask.id.split('!')[0]]
            if (profile.get(qtask.profile) != None) :
                required_datasets = self.bs.profiles[qtask.id.split('!')[0]][qtask.profile]['datasets']
            if (required_datasets != None and len(required_datasets) > 0 ):
                qboxes_list = self.storage_controller.get_storages_by_dataset(required_datasets)
            return qboxes_list

        def list_qboxes_by_download_time(self, qtask):
            ''' Lists QBoxes ordered by the predicted download time of the datasets '''

            required_datasets = {} # To get the list of datasets requireds by the job
            if (self.bs.profiles.get(qtask.profile) != None) :
                required_datasets = self.bs.profiles[qtask.profile]['datasets']
            if (len(required_datasets) > 0):
                # TODO
                qboxes_list = [] # self.storage_controller.get_storages_by_download_time(required_dataset)
            else:
                qboxes_list = []
            return qboxes_list

        def list_available_mobos(self, qboxes_list):
            ''' It receives the qboxes_list and returns a list wich all qmobos available from each qbox '''

            available_mobos_by_dataset = []
            for qb in qboxes_list:
                for mobo in self.lists_available_mobos:
                    if (qb.name == mobo[0]):
                            available_mobos_by_dataset.append(mobo)
            return available_mobos_by_dataset

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

            # TODO here we take care only of "regular" tasks with instances and not cluster tasks

            if len(self.qtasks_queue) == 0:
                self.logger.info("[{}]- QNode has nothing to dispatch".format(self.bs.time()))
                return

            # Sort the jobs by decreasing priority (hence the '-' sign) and then by increasing number of running instances
            self.logger.info("[{}]- QNode starting doDispatch".format(self.bs.time()))
            for qtask in sorted(self.qtasks_queue.values(),key=lambda qtask:(-qtask.priority, qtask.nb_dispatched_instances)):
                nb_instances_left = len(qtask.waiting_instances)
                if nb_instances_left > 0:
                    self.logger.debug("[{}]- QNode trying to dispatch {} of priority {} having {} waiting and {} dispatched instances".format(self.bs.time(),qtask.id, qtask.priority, len(qtask.waiting_instances), qtask.nb_dispatched_instances))
                    # Dispatch as many instances as possible on mobos available for bkgd, no matter the priority of the qtask
                    self.sortAvailableMobos("bkgd")
                    list_qboxes = self.list_qboxes_with_dataset(qtask)
                    if(len(list_qboxes) == 0):
                        list_available_mobos = self.lists_available_mobos
                    else:
                        list_available_mobos = self.list_available_mobos(list_qboxes)
                    for tup in list_available_mobos:
                        qb = self.dict_qboxes[tup[0]]
                        nb_slots = tup[1]
                        if nb_slots >= nb_instances_left:
                            # There are more available slots than instances, gotta dispatch'em all!
                            jobs = qtask.waiting_instances.copy()
                            self.addJobsToMapping(jobs, qb)                     # Add the Jobs to the internal mapping
                            qtask.instances_dispatched(jobs)                    # Update the QTask
                            qb.onDispatchedInstance(jobs, PriorityGroup.BKGD, qtask.id) # Dispatch the instances
                            tup[1] -= nb_instances_left                             # Update the number of slots in the list
                            nb_instances_left = 0
                            # No more instances are waiting, stop the dispatch for this qtask
                            break
                        elif nb_slots > 0: # 0 < nb_slots < nb_instances_left
                            # Schedule instances for all slots of this QBox
                            jobs = qtask.waiting_instances[0:nb_slots]
                            self.addJobsToMapping(jobs, qb)
                            qtask.instances_dispatched(jobs)
                            qb.onDispatchedInstance(jobs, PriorityGroup.BKGD, qtask.id)
                            tup[1] = 0
                            nb_instances_left -= nb_slots
                    #End for bkgd slots

                    if (nb_instances_left > 0) and (qtask.priority_group > PriorityGroup.BKGD):
                        # There are more instances to dispatch and the qtask is either low or high priority
                        self.sortAvailableMobos("low")
                        if(len(list_qboxes) == 0):
                            list_available_mobos = self.lists_available_mobos
                        else:
                            list_available_mobos = self.list_available_mobos(list_qboxes)


                        for tup in list_available_mobos:
                            qb = self.dict_qboxes[tup[0]]
                            nb_slots = tup[2]
                            if nb_slots >= nb_instances_left:
                                # There are more available slots than instances, gotta dispatch'em all!
                                jobs = qtask.waiting_instances.copy()
                                self.addJobsToMapping(jobs, qb)
                                qtask.instances_dispatched(jobs)
                                qb.onDispatchedInstance(jobs, PriorityGroup.LOW, qtask.id)
                                tup[2] -= nb_instances_left
                                nb_instances_left = 0
                                # No more instances are waiting, stop the dispatch for this qtask
                                break
                            elif nb_slots > 0: # 0 < nb_slots < nb_instances_left
                                # Schedule instances for all slots of this QBox
                                jobs = qtask.waiting_instances[0:nb_slots]
                                self.addJobsToMapping(jobs, qb)
                                qtask.instances_dispatched(jobs)
                                qb.onDispatchedInstance(jobs, PriorityGroup.LOW, qtask.id)
                                tup[2] = 0
                                nb_instances_left -= nb_slots

                        #End for low slots

                        if (nb_instances_left > 0) and (qtask.priority_group > PriorityGroup.LOW):
                            # There are more instances to dispatch and the qtask is high priority
                            self.sortAvailableMobos("high")
                            if(len(list_qboxes) == 0):
                                list_available_mobos = self.lists_available_mobos
                            else:
                                list_available_mobos = self.list_available_mobos(list_qboxes)

                            for tup in list_available_mobos:
                                qb = self.dict_qboxes[tup[0]]
                                nb_slots = tup[3]
                                if nb_slots >= nb_instances_left:
                                    # There are more available slots than wild instances, gotta catch'em all!
                                    jobs = qtask.waiting_instances.copy()
                                    self.addJobsToMapping(jobs, qb)
                                    qtask.instances_dispatched(jobs)
                                    qb.onDispatchedInstance(jobs, PriorityGroup.HIGH, qtask.id)
                                    tup[3] -= nb_instances_left
                                    nb_instances_left = 0

                                    # No more instances are waiting, stop the dispatch for this qtask
                                    break
                                elif nb_slots > 0: # 0 < nb_slots < nb_instances_left
                                    # Schedule instances for all slots of this QBox
                                    jobs = qtask.waiting_instances[0:nb_slots]
                                    self.addJobsToMapping(jobs, qb)
                                    qtask.instances_dispatched(jobs)
                                    qb.onDispatchedInstance(jobs, PriorityGroup.HIGH, qtask.id)
                                    tup[3] = 0
                                    nb_instances_left -= nb_slots
                            #End for high slots
                        #End if high priority and nb_instances_left > 0
                    #End if low/high priority and nb_instances_left > 0
                    self.logger.debug("[{}]- QNode now dispatched a total of {} instances of {}, {} are still waiting.".format(self.bs.time(),qtask.nb_dispatched_instances, qtask.id, len(qtask.waiting_instances)))
                #End if nb_instances_left > 0
            #End for qtasks in queue
            self.logger.info("[{}]- QNode end of doDispatch".format(self.bs.time()))
        #End of doDispatch function
