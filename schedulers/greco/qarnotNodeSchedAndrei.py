from batsim.batsim import BatsimScheduler, Batsim, Job
from StorageController import *
from qarnotUtilsAndrei import *
from qarnotBoxSchedAndrei import QarnotBoxSchedAndrei as QarnotBoxSched

from procset import ProcSet
from collections import defaultdict

import logging

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

                # For the manager
                self.dict_qboxes = {}        # Maps the QBox id to the QarnotBoxSched object
                self.dict_resources = {} # Maps the Batsim resource id to the QarnotBoxSched object

                # Dispatcher
                self.qtasks_queue = {}     # Maps the QTask id to the QTask object that is waiting to be scheduled
                self.jobs_mapping = {}     # Maps the batsim job id to the QBox Object where it has been sent to
                
                self.lists_available_mobos = [] # List of [qbox_name, slots bkgd, slots low, slots high]
                                                                                # This list is updated every 30 seconds from the reports of the QBoxes
                

                #NOT USED AT THE MOMENT:
                #self.qboxes_free_disk_space = {} # Maps the QBox id to the free disk space (in GB)
                #self.qboes_queued_upload_size = {} # Maps the QBox id to the queued upload size (in GB)

                self.update_period = 30 # The scheduler will be woken up by Batsim every 30 seconds
                self.time_next_update = 1.0 # The next time the scheduler should be woken up
                self.ask_next_update = False # Whether to ask for next update in onNoMoreEvents function
                self.very_first_update = True

                self.nb_rejected_jobs_by_qboxes = 0 # Just to keep track of the count

                self.max_simulation_time = 500


        def onSimulationBegins(self):
            assert self.bs.dynamic_job_registration_enabled, "Registration of dynamic jobs must be enabled for this scheduler to work"
            assert self.bs.allow_storage_sharing, "Storage sharing must be enabled for this scheduler to work"
            assert self.bs.ack_of_dynamic_jobs == False, "Acknowledgment of dynamic jobs must be disabled for this scheduler to work"
            #assert len(self.bs.air_temperatures) > 0, "Temperature option '-T 1' of Batsim should be set for this scheduler to work"
            # TODO maybe change the option to send every 30 seconds instead of every message of Batsim (see TODO list)

            # Register the profile of the cpu-burn jobs. CPU value is 1e20 because it's supposed to be endless and killed when needed.
            self.bs.register_profiles("dyn-burn", {"burn":{"type":"parallel_homogeneous", "cpu":1e20, "com":0}})
            self.bs.wake_me_up_at(1)

            self.initQBoxesAndStorageController()
            for qb in self.dict_qboxes.values():
                qb.onBeforeEvents()

            self.storage_controller.onSimulationBegins()

            self.logger.info("[{}]- QNode: End of SimulationBegins".format(self.bs.time()))

            self.end_of_simulation = False


        def onSimulationEnds(self):
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
                qb = QarnotBoxSched(qb_name, dict_qrads, self.bs, self, self.storage_controller)

                self.dict_qboxes[qb_name] = qb

                # Populate the mapping between batsim resource ids and the associated QarnotBoxSched
                for mobos_list in dict_qrads.values():
                    for (batid, _, _) in mobos_list:
                        self.dict_resources[batid] = qb

            self.nb_qboxes = len(self.dict_qboxes)
            self.nb_computing_resources = len(self.dict_resources)

            #Adding Data sets to test
            #print(" &&&&&&&&& Storages: ", self.storage_controller.get_storages())
            
            # Data sets for the simple workload
            #self.storage_controller.add_dataset(12, Dataset("QJOB-first:user-input:540624", 17))
            #self.storage_controller.add_dataset(12, Dataset("QJOB-first:docker:162852561", 18))
            #self.storage_controller.add_dataset(12, Dataset("QJOB-first:user-input:41428146", 19))

            # Data sets for the 1-day-qarnot-samples
            self.storage_controller.add_dataset(978, Dataset("QJOB-0225-1136-8f76-62245c536189:user-input:540624", 17))
            self.storage_controller.add_dataset(978, Dataset("QJOB-0225-1136-8f76-62245c536189:docker:162852561", 17))
            self.storage_controller.add_dataset(978, Dataset("QJOB-0225-1136-8f76-62245c536189:user-input:41428146", 17))

            self.storage_controller.add_dataset(980, Dataset("QJOB-0225-0908-c466-4492ab4e86f3:docker:54384684", 30))
            self.storage_controller.add_dataset(980, Dataset("QJOB-0225-0908-c466-4492ab4e86f3:user-input:41428146", 30))

            self.storage_controller.add_dataset(989, Dataset("QJOB-0225-0912-c5a0-3528fd7b7c12:docker:1305968769", 15))
            self.storage_controller.add_dataset(989, Dataset("QJOB-0225-0912-c5a0-3528fd7b7c12:user-input:41428146", 15))
            self.storage_controller.add_dataset(989, Dataset("QJOB-0225-0912-c5a0-3528fd7b7c12:user-input:0", 15))

            self.storage_controller.add_dataset(989, Dataset("QJOB-0225-1136-6d83-95be917ac327:user-input:41428146", 20))
            self.storage_controller.add_dataset(989, Dataset("QJOB-0225-1136-6d83-95be917ac327:user-input:0", 20))
            self.storage_controller.add_dataset(989, Dataset("QJOB-0225-1136-6d83-95be917ac327:docker:162852561", 20))
            #print("---Qboxes have a life :) ")

        def onRequestedCall(self):
            pass

        def onBeforeEvents(self):
            print("\n")

            if self.bs.time() >= self.time_next_update:
                self.logger.info("[{}]- QNode calling update on QBoxes".format(self.bs.time()))
                # It's time to ask QBoxes to update and report their state
                self.lists_available_mobos = []
                for qb in self.dict_qboxes.values():
                    # This will update the list of availabilities of the QMobos (and other QBox-related stuff)
                    tup = qb.updateAndReportState()
                    self.lists_available_mobos.append(tup)

                self.time_next_update = self.bs.time() + self.update_period

                if self.very_first_update: # First update at t=1, next updates every 30 seconds starting at t=30
                    self.time_next_update -= 1
                    self.very_first_update = False

                self.ask_next_update = True # We will ask for the next wake up in onNoMoreEvents if the simulation is not finished yet

                self.do_dispatch = True # We will do a dispatch anyway
            else:
                self.do_dispatch = False # We may not have to dispatch, depending on the events in this message

            for qb in self.dict_qboxes.values():
                qb.onBeforeEvents()


        def onNoMoreEvents(self):
            if self.do_dispatch:
                self.doDispatch()

            for qb in self.dict_qboxes.values():
                qb.onNoMoreEvents()

            # If the simulation is not finished and we need to ask Batsim for the next waking up
            if not self.isSimulationFinished() and self.ask_next_update:
                self.bs.wake_me_up_at(self.time_next_update)


        def onNotifyEventTargetTemperatureChanged(self, machines, new_temperature):
            for machine_id in machines:
                self.dict_resources[machine_id].onTargetTemperatureChanged(machine_id, new_temperature)

        def onNotifyEventOutsideTemperatureChanged(self, machines, new_temperature):
            pass

        #def onNotifyEventNewDatasetOnStorage(self, machines, dataset_id, dataset_size):
        #    self.storage_controller.onNotifyEventNewDatasetOnStorage(machines, dataset_id, dataset_size)

        def onNotifyEventMachineUnavailable(self, machines):
                for machine_id in machines:
                        self.dict_resources[machine_id].onNotifyMachineUnavailable(machine_id)

        def onNotifyEventMachineAvailable(self, machines):
                for machine_id in machines:
                        self.dict_resources[machine_id].onNotifyMachineAvailable(machine_id)


        def onJobSubmission(self, job):
            print('     ---> Submitting job: ', job.id)
            qtask_id = job.id.split('_')[0] # It is the formar workload_id!name_of_job. For example: 278891!QJOB-second_0
            qtask_profile = job.profile
            job.qtask_id = qtask_id


            # Retrieve or create the corresponding QTask
            if not qtask_id in self.qtasks_queue:
                #TODO, add the job.profile as parameter
                qtask = QTask(qtask_id, qtask_profile, job.profile_dict["priority"])
                self.qtasks_queue[qtask_id] = qtask
            else:
                qtask = self.qtasks_queue[qtask_id]

            qtask.instance_submitted(job)
            self.do_dispatch = True


        '''def onRejectedInstance(self, job):
            #Should not happen a lot of times
            self.nb_rejected_jobs_by_qboxes += 1
            qb = self.jobs_mapping.pop(job.id)
            self.logger.info("The job {} was rejected by QBox {}".format(job.id, qb.name))
            self.qtasks_queue[job.qtask_id].instance_rejected(job)'''


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
                        self.logger.info("[{}]- QNode asked direct dispatch of {} on QBox {}".format(direct_job.id, qb.name))
                        qb.onJobCompletion(job, direct_job)

                    else:
                        qb.onJobCompletion(job)
                        # A slot should be available, do a general dispatch
                        self.do_dispatch = True

                        #Check if the QTask is complete
                        if qtask.is_complete():
                            self.logger.info("[{}]    All instances of QTask {} have terminated".format(self.bs.time(), qtask.id))
                            del self.qtasks_queue[qtask.id]


                else:
                    # "Regular" instances are not supposed to have a walltime nor fail
                    assert False, "Job {} reached the state {}, this should not happen.".format(job.id, job.job_state)
            #End if/else on job.workload
        #End onJobCompletion

        def onJobsKilled(self,job):
            pass
            #TODO pass?


        def isSimulationFinished(self):
            # TODO This is a guard to avoid infinite loops.
            if (self.bs.time() > self.max_simulation_time or
                              (self.bs.no_more_static_jobs and self.bs.no_more_external_events and len(self.qtasks_queue) == 0 and len(self.jobs_mapping) == 0) ):
                self.end_of_simulation = True
                self.bs.notify_registration_finished() # TODO this may not have its place here
                return True
            else:
                return False


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

            print("         ---> Searching qboxes by datasets")
            required_datasets = {} # To get the list of datasets requireds by the job
            qboxes_list = []
            print("         ---> Profiles on system",self.bs.profiles)
            profile = self.bs.profiles[qtask.id.split('!')[0]]
            if (profile.get(qtask.profile) != None) :
                print("         ---> It is in the system")
                required_datasets = self.bs.profiles[qtask.id.split('!')[0]][qtask.profile]['datasets']
            if (required_datasets != None and len(required_datasets) > 0 ):
                print("         ---> The required Data Sets:", required_datasets)
                qboxes_list = self.storage_controller.get_storages_by_dataset(required_datasets)

            print("--------------------------------------------------------------------------")
            print("List of candidate qboxes by data sets location: ", qboxes_list)
            print("--------------------------------------------------------------------------")
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

            print("--------------------------------------------------------------------------")
            print("List of candidate qboxes by download time: ", qboxes_list)
            print("--------------------------------------------------------------------------")
            return qboxes_list
        
        def next_profile_index(self):
            ''' Return the start index for the next profiles '''
            qtask_queue_by_profile = sorted(self.qtasks_queue.values(),key=lambda qtask:(qtask.profile))
            index = 0
            last_profile = ""
            for task in qtask_queue_by_profile:
                if (task.profile == last_profile):
                    index = index + 1
                else:
                    break
                last_profile = task.profile
            return index

        def list_available_mobos(self, qboxes_list):
            ''' It receives the qboxes_list and returns a list wich all qmobos available from each qbox '''
            print("         ---> Searching for the availale Mobos: ")
            print("             ---> The big list: ", self.lists_available_mobos)
            for item in self.lists_available_mobos:
                print ("                ---> Item: ", item)
            available_mobos_by_dataset = []
            #mobos_names = self.lists_available_mobos['names']
            for qb in qboxes_list:
                print("              ---> Seaching for: ", qb.name)
                for mobo in self.lists_available_mobos:
                    if (qb.name == mobo[0]):
                        print("                 ---> QMobo found")
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

            # Sort the jobs by decreasing priority (hence the '-' sign) and then by increasing number of running instances
            self.logger.info("[{}]- QNode starting doDispatch".format(self.bs.time()))
            for qtask in sorted(self.qtasks_queue.values(),key=lambda qtask:(-qtask.priority, qtask.nb_dispatched_instances)):
                self.logger.info("[{}]- QNode trying to dispatch {} of priority {} having {} dispatched instances".format(self.bs.time(),qtask.id, qtask.priority, qtask.nb_dispatched_instances))
                nb_instances_left = len(qtask.waiting_instances)
                print(" ---> {} Instances left", nb_instances_left)
                if nb_instances_left > 0:
                    # Dispatch as many instances as possible on mobos available for bkgd, no matter the priority of the qtask
                    print("****************************************************\n Bkgd")
                    self.sortAvailableMobos("bkgd")
                    print("     ---> Searching Qboxes with the required Data sets")
                    list_qboxes = self.list_qboxes_with_dataset(qtask)                  # Build the list of Qboxes by dataset location
                    if(len(list_qboxes) == 0):
                        print("     ---> No one Qboxes with the data sets, lets check the download time")                                          # No one Qbox has the dataset
                        list_qboxes = self.list_qboxes_by_download_time(qtask)          # Build the list of Qboxes by the predicted download time of the datasets
                    else:
                        print("     ---> We found some Qboxes")
                    if(len(list_qboxes) == 0):
                        print("     ---> No prediction of time") 
                        list_available_mobos = self.lists_available_mobos
                    else:
                        print("     ---> Getting the available mobos")
                        list_available_mobos = self.list_available_mobos(list_qboxes)
                        print("         ---> The available mobos: ", list_available_mobos)
                    

                    for tup in list_available_mobos:
                        print("     ---> Into tups")
                        qb = self.dict_qboxes[tup[0]]
                        nb_slots = tup[1]
                        print("     ---> This tup offers: ", nb_slots)
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
                        print("****************************************************\n Low")
                        self.sortAvailableMobos("low")
                        
                        list_qboxes = self.list_qboxes_with_dataset(qtask)                  # Build the list of Qboxes by dataset location
                        if(len(list_qboxes) == 0):
                            print("     ---> No one Qboxes with the data sets, lets check the download time")                                          # No one Qbox has the dataset
                            list_qboxes = self.list_qboxes_by_download_time(qtask)          # Build the list of Qboxes by the predicted download time of the datasets
                        else:
                            print("     ---> We found some Qboxes")
                        if(len(list_qboxes) == 0):
                            print("     ---> No prediction of time") 
                            list_available_mobos = self.lists_available_mobos
                        else:
                            print("     ---> Getting the available mobos")
                            list_available_mobos = self.list_available_mobos(list_qboxes)
                            print("         ---> The available mobos: ", list_available_mobos)
                        
                        for tup in list_available_mobos:
                            qb = self.dict_qboxes[tup[0]]
                            nb_slots = tup[2]
                            print("     ---> This tup offers: ", nb_slots)
                            print("     ---> We will dispatch the task here: ")
                            print("     ---> We need: ", nb_instances_left)

                            if nb_slots >= nb_instances_left:
                                print("     ---> Dispatch all ")
                                # There are more available slots than instances, gotta dispatch'em all!
                                jobs = qtask.waiting_instances.copy()
                                self.addJobsToMapping(jobs, qb)
                                qtask.instances_dispatched(jobs)
                                qb.onDispatchedInstance(jobs, PriorityGroup.LOW, qtask.id)
                                tup[2] -= nb_instances_left
                                nb_instances_left = 0
                                print("     ---> Dispatched! ")
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
                            print("****************************************************\n High")
                            self.sortAvailableMobos("high")
                            
                            list_qboxes = self.list_qboxes_with_dataset(qtask)                  # Build the list of Qboxes by dataset location
                            if(len(list_qboxes) == 0):
                                print("     ---> No one Qboxes with the data sets, lets check the download time")                                          # No one Qbox has the dataset
                                list_qboxes = self.list_qboxes_by_download_time(qtask)          # Build the list of Qboxes by the predicted download time of the datasets
                            else:
                                print("     ---> We found some Qboxes")
                            if(len(list_qboxes) == 0):
                                print("     ---> No prediction of time") 
                                list_available_mobos = self.lists_available_mobos
                            else:
                                print("     ---> Getting the available mobos")
                                list_available_mobos = self.list_available_mobos(list_qboxes)
                                print("         ---> The available mobos: ", list_available_mobos)
                            
                            for tup in list_available_mobos:
                                qb = self.dict_qboxes[tup[0]]
                                nb_slots = tup[3]
                                print("     ---> This tup offers: ", nb_slots)
                                if nb_slots >= nb_instances_left:
                                    print("     ---> We will dispatch the task here: ")
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
                #End if nb_instances_left > 0
                self.logger.info("[{}]- QNode now dispatched a total of {} instances of {}".format(self.bs.time(),qtask.nb_dispatched_instances, qtask.id))
            #End for qtasks in queue
            self.logger.info("[{}]- QNode end of doDispatch".format(self.bs.time()))
        #End of doDispatch function
