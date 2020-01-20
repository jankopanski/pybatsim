from batsim.batsim import Batsim, Job
from qarnotUtils import *

from procset import ProcSet
from collections import defaultdict

import logging


'''
This is the qarnot QBox scheduler


List of available mobos:
- availBkgd: for each QBox number of mobos that are too cold and are running CPU burn tasks to heat
  We start CPU burn if air_temp < target - 1                       start if diff >= 1
  We stop the CPU burn if air_temp > target + 1                    stop if  diff < -1
  We can start low and high priority tasks here as well

- availLow: for each QBox number of mobos that are too hot to run cpu burn and are probably idle (or running cpu burn)
  We can start low priority tasks if air_temp < target + 1         start low if diff >= -1
  We stop the low priority tasks there if air_temp > target + 3    stop low if  diff < -3
  We can start high priority tasks here as well

- availHigh: for each QBox number of mobos that are too hot to run low priority tasks
  We can start high priority tasks if air_temp < target + 4        start high if diff >= -4
  We stop high priority tasks if air_temp > target + 10            stop high  if diff < -10


When a task of higher priority is sent to a mobo that is already running something,
wait for all the datasets to arrive before stopping the execution of the current task.

#TODO need to add a "warmup" time for booting the mobo when a new task is executed on it

'''

class QarnotBoxSched():
    def __init__(self, name, dict_qrads, site, bs, qn, storage_controller):
        ''' WARNING!!!
        The init of the QBox Schedulers is done upon receiving
        the SimulationBegins in the QNode Scheduler 
        Thus there is no onSimulationBegins called for a QBox sched
        '''
        self.bs = bs                                    # Batsim
        self.qn = qn                                    # The QarnotNodeSched
        self.storage_controller = storage_controller    # The StorageController
        self.logger = bs.logger                         # The logger
        self.name = name                                # QBox qguid
        self.site = site                                # Location of the QBox, either "paris" or "bordeaux" or "Reau"

        self.dict_qrads = {}     # Maps the qrad_names to QRad object
        self.dict_ids = {}       # Maps the batids of the mobos to the QRad object that contains it

        # Global counts of all mobos under my watch
        self.mobosAvailable = ProcSet()       # Whether the mobos are available or not (from the QRad hotness and external events point of view)
        self.mobosUnavailable = ProcSet()     # Mobos unavailable due to the QRad being too warm (or from external events)

        # TODO maybe we can directly use lists of QRads, may be easier to sort by temperature and get the coolest/warmest when scheduling an instance
        self.availBkgd = ProcSet()
        self.availLow = ProcSet()
        self.availHigh = ProcSet()

        for qr_name, mobos_list in dict_qrads.items():
            qr = QRad(qr_name, bs)
            self.dict_qrads[qr_name] = qr

            # Get properties of the first mobo, since all mobos should be identical
            properties = mobos_list[0][2]
            watts = (properties["watt_per_state"]).split(',')
            properties["watt_per_state"] = [float((x.split(':'))[-1]) for x in watts]
            properties["nb_pstates"] = len(watts)
            qr.properties = properties

            # Create the QMobos
            max_pstate = properties["nb_pstates"]-1
            dict_mobos = {}
            for (batid, mobo_name, properties) in mobos_list:
                dict_mobos[batid] = QMobo(mobo_name, batid, max_pstate)
                self.dict_ids[batid] = qr

                if (properties["temperature_role"] == "master"):
                    qr.temperature_master = batid

            qr.dict_mobos = dict_mobos
            qr.pset_mobos = ProcSet(*dict_mobos.keys())

        # Assume all mobos are available for LOW tasks at the beginning (and not BKGD to not start a cpu burn on every resource at t=0)
        self.mobosAvailable = ProcSet(*self.dict_ids.keys())
        self.availLow = ProcSet(*self.dict_ids.keys())
        self.nb_mobos = len(self.dict_ids)

        self.stateChanges = defaultdict(ProcSet) # Keys are target pstate, values are list of resources for which we change the state
        self.jobs_to_kill = []
        self.jobs_to_execute = []
        self.burning_jobs = set()

        self.dict_subqtasks = {} # Maps the QTask id to the SubQTask object
        self.waiting_datasets = [] # List of datasets for which data staging has been asked
                                   # If a dataset appears multiple times in the list, it means that multiple QTasks are waiting for this dataset
        self.dict_reserved_jobs_mobos = defaultdict(list) # Maps a sub_qtask id to a list of reservation pairs (qm, job)

        # Tells the StorageController who we are and retrieve the batid of our disk
        self.disk_batid = self.storage_controller.onQBoxRegistration(self.name, self)

        self.logger.info("[{}]--- {} init'd correctly. Night gathers, and now my watch on {} Qrads and {} QMobos begins!".format(self.bs.time(),self.name, len(self.dict_qrads.keys()), self.nb_mobos))

    def onSimulationEnds(self):
        pass

    def onBeforeEvents(self):
        pass

    def onNoMoreEvents(self):
        if self.bs.time() >= 1.0:
            self.doFrequencyRegulation()

        if len(self.jobs_to_kill) > 0:
            self.bs.kill_jobs(self.jobs_to_kill)
            self.jobs_to_kill = []
        if len(self.jobs_to_execute) > 0:
            self.bs.execute_jobs(self.jobs_to_execute)
            self.jobs_to_execute = []

        # Then append all SET_RESOURCE_STATE events that occured during the frequency regulation
        for pstate, resources in self.stateChanges.items():
            self.bs.set_resource_state(resources, pstate)
        self.stateChanges.clear()


    def onTargetTemperatureChanged(self, qrad_name, new_temperature):
        qr = self.dict_qrads[qrad_name]
        qr.targetTemp = new_temperature
        qr.diffTemp = new_temperature - self.bs.air_temperatures[str(qr.temperature_master)]


    def onNotifyMachineUnavailable(self, machines):
        # The QRad became too hot (from external event)
        # Then mark this machine as unavailable
        # If instances were running on these machines, they will be killed during the frequency regulation
        self.logger.info(f"[{self.bs.time()}] New unavailable mobos: {machines} to be removed to {self.mobosAvailable}")
        self.mobosAvailable.difference_update(machines)
        self.mobosUnavailable.insert(machines)
        # TODO need to kill the instance that was running on this mobo and re-submit it.

    def onNotifyMachineAvailable(self, machines):
        # Put the machine back available
        self.logger.info(f"[{self.bs.time()}] New available mobos: {machines} to be added to {self.mobosAvailable}")
        self.mobosAvailable.insert(machines)
        self.mobosUnavailable.difference_update(machines)


    def killOrRejectAllJobs(self):
        '''
        'stop_simulation' event has been received. Returns the lists of jobs to reject and to kill.
        '''
        to_reject = []
        to_kill = list(self.burning_jobs)
        to_kill.extend(self.jobs_to_kill) # There may be jobs being killed during the UpdateAndReportState

        for sub_qtask in self.dict_subqtasks.values():
            to_reject.extend(sub_qtask.waiting_instances)
            to_reject.extend(sub_qtask.reserved_instances)
            to_kill.extend(sub_qtask.running_instances)

        return (to_reject, to_kill)


    def updateAndReportState(self):
        '''
        The state of the QBox is updated every *qnode_sched.update_period* seconds, or before the QNode performs a dispatch.
        The temperature of the QRads is checked and decisions are taken to kill an instance if the rad is too hot
        Then, the list of mobos available for each priority group is updated
        and returned back to the QNode.
        The frequency regulation will be done when all events are treated, called in onNoMoreEvents
        Returns a list [qbox_name, slots bkgd, slots low, slots high]
        The sum of the slots for bkgd, low and high equals the total number of mobos that are not reserved or running instances.
        '''
        self.availBkgd.clear()
        self.availLow.clear()
        self.availHigh.clear()

        jobs_to_kill = set()
        for qr in self.dict_qrads.values():
            qr.diffTemp = qr.targetTemp - self.bs.air_temperatures[str(qr.temperature_master)]
            #self.logger.debug("[{}]----- QRad {} target: {}, air: {}, diff: {}".format(self.bs.time(), qr.name, qr.targetTemp, self.bs.air_temperatures[str(qr.temperature_master)], qr.diffTemp))
            # Check if we have to kill instances
            if qr.diffTemp < -10:
                # QRad is too hot to run HIGH LOW and BKGD, gotta kill'em all!
                for qm in qr.dict_mobos.values():
                    if qm.running_job != -1:
                        job = qm.pop_job()
                        jobs_to_kill.add(job)
                        self.logger.debug("[{}] Asked to kill {} of priority {} because {} too hot ({} < -10)".format(self.bs.time(), job.id, job.profile_dict["priority"], qr.name, qr.diffTemp))

            elif  qr.diffTemp < -3:
                # QRad is too hot to run LOW and BKGD, kill instances if any
                for qm in qr.dict_mobos.values():
                    if qm.state == QMoboState.RUNLOW or qm.state == QMoboState.RUNBKGD:
                        job = qm.pop_job()
                        jobs_to_kill.add(job)
                        self.logger.debug("[{}] Asked to kill {} of priority {} because {} too hot ({} < -3) for LOW BKGD".format(self.bs.time(), job.id, job.profile_dict["priority"], qr.name, qr.diffTemp))

            elif qr.diffTemp < -1:
                # QRad is too hot to run BKGD, kill those CPU burns!
                for qm in qr.dict_mobos.values():
                    if qm.state == QMoboState.RUNBKGD:
                        job = qm.pop_job()
                        jobs_to_kill.add(job)
                        self.logger.debug("[{}] Asked to kill {} of priority {} because {} too hot ({} < -1) for BKGD".format(self.bs.time(), job.id, job.profile_dict["priority"], qr.name, qr.diffTemp))

            for qm in qr.dict_mobos.values():
                if (qr.diffTemp >= 1) and (qm.state < QMoboState.RUNBKGD) and not qm.is_reserved():
                    # This mobo is available for BKGD instances
                    self.availBkgd.insert(qm.batid)
                elif (qr.diffTemp >= -1) and (qm.state < QMoboState.RUNLOW) and not qm.is_reserved():
                    # This mobo is available for LOW instances
                    self.availLow.insert(qm.batid)
                elif (qr.diffTemp >= -4) and (qm.state < QMoboState.RUNHIGH) and not qm.is_reserved_high():
                    # This mobo is available for HIGH
                    self.availHigh.insert(qm.batid)


        if len(jobs_to_kill) > 0:
            #self.logger.info("[{}]--- {} asked to kill the following jobs during updateAndReportState: {}".format(self.bs.time(), self.name, jobs_to_kill))
            self.logger.info("[{}]--- {} asked to kill {} jobs during updateAndReportState".format(self.bs.time(), self.name, len(jobs_to_kill)))
            #for qr in self.dict_qrads.values():
            #    self.logger.info("[{}]----- QRad {} {} target: {}, air: {}, diff: {}".format(self.bs.time(), qr.name, qr.temperature_master, qr.targetTemp, self.bs.air_temperatures[str(qr.temperature_master)], qr.diffTemp))
            self.jobs_to_kill.extend(jobs_to_kill)
            self.burning_jobs.difference_update(jobs_to_kill)

        # Filter out mobos that are marked as unavailable
        self.availBkgd -= self.mobosUnavailable
        self.availLow -= self.mobosUnavailable
        self.availHigh -= self.mobosUnavailable


        self.logger.info("[{}]--- {} reporting the available slots for bkgd/low/high: {}/{}/{}".format(self.bs.time(), self.name, len(self.availBkgd), len(self.availLow), len(self.availHigh)))
        return [self.name, len(self.availBkgd), len(self.availLow), len(self.availHigh)]


    def onDispatchedInstance(self, instances, priority_group, qtask_id, is_cluster = False):
        '''
        Instances is a list of Batsim jobs corresponding to the instances dispatched to this QBox.
        Priority_group of the instances

        Datasets are shared between the instances of the same QTask. So we only need to retrive the datasets once for all instances

        If the instances list has only one instance, it may be a cluster task. In that case, it requires more than one resource.
        '''
        if is_cluster:
            # This is a cluster task requiring multiple resources
            self.logger.info("[{}]--- {} received a cluster task {} for the priority group {}".format(self.bs.time(), self.name, qtask_id, priority_group))
            assert qtask_id not in self.dict_subqtasks, f"{self.name} received a cluster task {qtask_id} but other instances of this cluster were received previously, aborting."

            # First check if there is enough available mobos
            nb_requested_res = instances[0].requested_resources
            if (priority_group == PriorityGroup.HIGH) and (len(self.mobosAvailable) < nb_requested_res) or \
               (priority_group < PriorityGroup.HIGH) and ( (len(self.availBkgd) + len(self.availLow)) < nb_requested_res):
                self.logger.info(f"[{self.bs.time()}]--- {self.name} does not have available mobos for cluster taks {qtask_id}, rejecting it.")
                self.qn.onQBoxRejectedInstances(instances, self.name)

            batjob = instances[0]
            sub_qtask = SubQTask(qtask_id, priority_group, instances,
                                 self.bs.profiles[batjob.workload][batjob.profile]["datasets"])
            sub_qtask.cluster_task = True
            self.logger.debug(f"[{self.bs.time()}] == Adding SubQTask Cluster {qtask_id} in the dict of qb {self.name}")
            self.dict_subqtasks[qtask_id] = sub_qtask

        else:
            #This is a regular dispatch of instances
            self.logger.info("[{}]--- {} received {} instances of {} for the priority group {}".format(self.bs.time(), self.name, len(instances), qtask_id, priority_group))
            if qtask_id in self.dict_subqtasks:
                # Some instances of this QTask have already been received by this QBox
                sub_qtask = self.dict_subqtasks[qtask_id]
                sub_qtask.waiting_instances.extend(instances.copy()) #TODO maybe don't need this copy since we do extend
            else:
                # This is a QTask "unknown" to the QBox.
                # Create and add the SubQTask to the dict
                sub_qtask = SubQTask(qtask_id, priority_group, instances.copy(),
                                     self.bs.profiles[instances[0].workload][instances[0].profile]["datasets"])
                self.logger.debug(f"[{self.bs.time()}] == Adding SubQTask {qtask_id} in the dict of qb {self.name}")
                self.dict_subqtasks[qtask_id] = sub_qtask

        # Then ask for the data staging of the required datasets
        # Even if all datasets were on disk before, that doesn't mean they are still there
        new_waiting_datasets = []
        for dataset_id in sub_qtask.datasets:
            if self.storage_controller.onQBoxAskDataset(self.disk_batid, dataset_id):
                # The dataset is already on disk, ask for a hardlink
                self.storage_controller.onQBoxAskHardLink(self.disk_batid, dataset_id, sub_qtask.id)
            else:
                # The dataset is not on disk yet, put it in the lists of waiting datasets
                new_waiting_datasets.append(dataset_id)
                self.waiting_datasets.append(dataset_id)
        sub_qtask.update_waiting_datasets(new_waiting_datasets)

        # Then schedule the instances
        if sub_qtask.is_cluster():
            self.scheduleClusterInstance(sub_qtask)
        else:
            self.scheduleInstances(sub_qtask)


    def scheduleInstances(self, sub_qtask):
        '''
        All datasets were already requested.
        Make reservations of mobos for each instance and then start them if the datasets are already on disk.
        Execute an instance HIGH on the coolest QRad that is available for HIGH (if possible without preempting LOW instance, don't care about BKGD)
        Execute an instance BKGD/LOW on the warmest QRad that is available for BKGD/LOW (preempt BKGD task if necessary)
        '''
        datasets_on_disk = (sub_qtask.waiting_datasets == []) # If no waiting datasets, they are all on disk
        n = len(sub_qtask.waiting_instances)
        if sub_qtask.priority_group == PriorityGroup.HIGH:
            # Find coolest QRad which is not running LOW instance, i.e. the QRad with the greatest diffTemp
            running_low = [] # List of mobos that are running LOW instances
            available_slots = self.availBkgd | self.availLow | self.availHigh # Get all mobos on which we can start a HIGH instance
            available_slots.difference_update(self.mobosUnavailable)
            qr_list = sorted(self.dict_qrads.values(), key=lambda qr:-qr.diffTemp) # Take QRads by decreasing temperature difference (i.e., increasing heating capacity)
            for qr in qr_list:
                for batid in (qr.pset_mobos & available_slots):
                    qm = qr.dict_mobos[batid]
                    if (qm.state <= QMoboState.RUNBKGD) and not qm.is_reserved_high(): # Either OFF/IDLE or running BKGD and not reserved for HIGH instance
                        job = sub_qtask.pop_waiting_instance()
                        if datasets_on_disk:
                            #Start the instance immediately
                            sub_qtask.mark_running_instance(job)
                            self.startInstance(qm, job)
                        else:
                            # Make a reservation
                            assert job not in self.dict_reserved_jobs_mobos, "A mobo is already reserved for this instance"
                            self.reserveInstance(qm, job)
                            sub_qtask.mark_reserved_instance(job)
                            self.logger.info("[{}] Adding reservation for {} on {} ({})".format(self.bs.time(), job.id, qm.name, qm.batid))
                        n-=1
                        if n == 0:
                            return # All instances have been scheduled
                    elif (qm.state == QMoboState.RUNLOW):
                        running_low.append(batid)
            # There are still instances to start, take mobos that are running LOW instances.
            for batid in running_low: # Mobos in this list are already sorted by coolest QRad first
                job = sub_qtask.pop_waiting_instance()
                qm = self.dict_ids[batid].dict_mobos[batid]
                if datasets_on_disk:
                    sub_qtask.mark_running_instance(job)
                    self.startInstance(qm, job)
                else:
                    # Make a reservation
                    assert job not in self.dict_reserved_jobs_mobos, "A mobo is already reserved for this instance"
                    self.reserveInstance(qm, job)
                    sub_qtask.mark_reserved_instance(job)
                    self.logger.info("[{}] Adding reservation for {} on {} ({})".format(self.bs.time(), job.id, qm.name, qm.batid))
                n-=1
                if n == 0:
                    return # All instances have been scheduled

        else: # This is a LOW instance
            # Find warmest QRad among the availLow and availBkgd
            available_slots = self.availBkgd | self.availLow # Get all mobos in which we can start a LOW instance
            available_slots.difference_update(self.mobosUnavailable)
            qr_list = sorted(self.dict_qrads.values(), key=lambda qr:qr.diffTemp) # Take QRads by increasing temperature difference (i.e., decreasing heat capacity)
            for qr in qr_list:
                for batid in (qr.pset_mobos & available_slots):
                    qm = qr.dict_mobos[batid]
                    if (qm.state <= QMoboState.RUNBKGD) and not qm.is_reserved(): # Either OFF/IDLE or running BKGD and not reserved
                        job = sub_qtask.pop_waiting_instance()
                        if datasets_on_disk:
                            sub_qtask.mark_running_instance(job)
                            self.startInstance(qm, job)
                        else:
                            # Make a reservation
                            assert job not in self.dict_reserved_jobs_mobos, "A mobo is already reserved for this instance"
                            self.reserveInstance(qm, job)
                            sub_qtask.mark_reserved_instance(job)
                            self.logger.info("[{}] Adding reservation for {} on {} ({})".format(self.bs.time(), job.id, qm.name, qm.batid))
                        n-=1
                        if n == 0:
                            return # All instances have been scheduled


        # Some instances were dispatched but cannot be scheduled yet, return them to the QNode
        self.logger.info("[{}]--- {} still has {} instances of {} to start, rejecting these instances back to the QNode but this should not happen.".format(self.bs.time(), self.name, len(sub_qtask.waiting_instances), sub_qtask.id))
        self.logger.info("[{}]--- {} has available slots for bkgd/low/high: {}/{}/{}".format(self.bs.time(), self.name, len(self.availBkgd), len(self.availLow), len(self.availHigh)))

        assert len(sub_qtask.waiting_instances) > 0, "QBox wants to reject 0 instances to the QNode..."
        self.qn.onQBoxRejectedInstances(sub_qtask.waiting_instances.copy(), self.name) # TODO maybe we don't need to copy this
        sub_qtask.waiting_instances = []


    def scheduleClusterInstance(self, sub_qtask):
        '''
        As for regular instances, all datasets were already requested.
        Make reservations for all mobos requested by this cluster or reject it and then start them if the datasets are already on disk.
        For HIGH priority, take mobos on the coolest QRads that are available for HIGH (if possible without preempting LOW instance, don't care about BKGD)
        For BKGD/LOW priority, take mobos on the warmest QRads that are available for BKGD/LOW (preempt BKGD task if necessary)
        '''
        job = sub_qtask.pop_waiting_instance()
        allocation = ProcSet()
        remaining_requested_res = job.requested_resources
        # At this moment, we are sure there is enough available mobos for this cluster, reserve them
        if sub_qtask.priority_group == PriorityGroup.HIGH:
            # Sort the QRads by coolest first and assign all available mobos to the cluster
            available_slots = self.availBkgd | self.availLow | self.availHigh
            available_slots.difference_update(self.mobosUnavailable)
            qr_list = sorted(self.dict_qrads.values(), key=lambda qr:-qr.diffTemp)
            for qr in qr_list:
                for batid in (qr.pset_mobos & available_slots): # Available mobos of this QRad
                    qm = qr.dict_mobos[batid]
                    if (qm.state <= QMoboState.RUNBKGD) and not qm.is_reserved_high():
                        allocation.insert(batid)
                        remaining_requested_res-=1
                        if remaining_requested_res == 0:
                            break

                if remaining_requested_res == 0:
                    break
            # End for qr in qr_list
        else:
            # This is a LOW priority cluster, sort QRads by warmest first
            available_slots = self.availBkgd | self.availLow
            available_slots.difference_update(self.mobosUnavailable)
            qr_list = sorted(self.dict_qrads.values(), key=lambda qr:qr.diffTemp)
            for qr in qr_list:
                for batid in (qr.pset_mobos & available_slots):
                    qm = qr.dict_mobos[batid]
                    if (qm.state <= QMoboState.RUNBKGD) and not qm.is_reserved():
                        allocation.insert(batid)
                        remaining_requested_res-=1
                        if remaining_requested_res == 0:
                            break

                if remaining_requested_res == 0:
                    break
            # End for qr in qr_list

        assert len(allocation) == job.requested_resources, f"Found only {len(allocation)} available mobos for cluster {sub_qtask.id} out of {job.requested_resources}."

        if sub_qtask.waiting_datasets == []:
            # We can start the cluster now
            sub_qtask.mark_running_instance(job)
            self.startCluster(allocation, job)
            self.logger.info(f"[{self.bs.time()}] Successfully started cluster {sub_qtask.id} on {len(allocation)} mobos")
        else:
            # Make reservations
            for batid in allocation:
                qm = self.dict_ids[batid].dict_mobos[batid]
                self.reserveInstance(qm, job)
            sub_qtask.mark_reserved_instance(job)
            self.logger.info("[{}] Adding reservation for cluster {} on {} mobos ({})".format(self.bs.time(), sub_qtask.id, len(allocation), str(allocation)))



    def onDatasetArrivedOnDisk(self, dataset_id):
        '''
        The Storage Controller notifies that a required dataset arrived on the disk.
        Ask for a hard link on this dataset if there are tasks that were waiting for this dataset.
        Then check if we can launch instances.
        '''
        if dataset_id in self.waiting_datasets:
            n = self.waiting_datasets.count(dataset_id)
            self.logger.info("[{}]--- Dataset {} arrived on QBox {} and was waited by {} SubQTasks".format(self.bs.time(), dataset_id, self.name, n))

            entries_to_remove = []
            for subqtask_id in self.dict_reserved_jobs_mobos.keys():
                if subqtask_id not in self.dict_subqtasks:
                    continue # Guard since the dict of reservations is a defaultdict

                sub_qtask = self.dict_subqtasks[subqtask_id]
                if dataset_id in sub_qtask.waiting_datasets:
                    sub_qtask.waiting_datasets.remove(dataset_id)
                    self.waiting_datasets.remove(dataset_id)
                    self.storage_controller.onQBoxAskHardLink(self.disk_batid, dataset_id, sub_qtask.id)
                    if len(sub_qtask.waiting_datasets) == 0:
                        # The SubQTask has all the datasets, launch the reservations that have been made
                        reservations = self.dict_reserved_jobs_mobos[subqtask_id].copy()

                        if sub_qtask.is_cluster():
                            self.logger.info(f"[{self.bs.time()}] {self.name} starting cluster {sub_qtask.id} on {len(reservations)} reserved mobos")
                            job = reservations[0][1] # All pairs in reservations have the same job since it's a cluster

                            assert job in sub_qtask.reserved_instances, f"A reservation for {job.id} was made but it was not marked as reserved in the SubQTask."
                            list_alloc = []
                            for pair in reservations:
                                pair[0].reserved_job = -1
                                list_alloc.append(pair[0].batid)

                            allocation = ProcSet(*list_alloc)
                            sub_qtask.mark_running_instance(job)
                            self.startCluster(allocation, job)
                            entries_to_remove.append(subqtask_id)

                        else: # It's a qtask with instances
                            for pair in reservations:
                                self.logger.info("[{}] {} starting {} on reserved mobo {} {}".format(self.bs.time(), self.name, pair[1].id, pair[0].batid, pair[0].name))
                                assert pair[1] in sub_qtask.reserved_instances, f"A reservation for {pair[1].id} was made but it was not marked as reserved in the SubQTask."
                                pair[0].reserved_job = -1
                                sub_qtask.mark_running_instance(pair[1])
                                self.startInstance(pair[0], pair[1])
                                self.dict_reserved_jobs_mobos[subqtask_id].remove(pair)
                    #else
                    #   There are more datasets waited by this SubQTask, so do nothing
                    n-= 1
                    if n == 0:
                        # All SubQTasks waiting for this dataset were found, stop
                        assert dataset_id not in self.waiting_datasets # TODO remove this at some point?
                        break

            for entry in entries_to_remove:
                del self.dict_reserved_jobs_mobos[entry]
        #else
        # The dataset is no longer required by a QTask. Do nothing


    def startCluster(self, allocation, job):
        '''
        Allocation is a ProcSet containing the batid of the mobos.
        Does pretty much the same as startInstances but for a cluster job.
        '''
        for batid in allocation:
            qm = self.dict_ids[batid].dict_mobos[batid]
            if qm.reserved_job != -1:
                self.logger.debug(f"[{self.bs.time()}] +++++ Rejecting {qm.reserved_job.id} to run cluster {job.id}")
                self.rejectReservedInstance(qm)
            if qm.running_job != -1:
                self.logger.debug(f"[{self.bs.time()}]------ Mobo {qm.name} {qm.batid} killed Job {qm.running_job.id} because a cluster arrived")
                old_job = qm.pop_job()
                self.jobs_to_kill.append(old_job)
                self.burning_jobs.discard(old_job)

            qm.push_job(job)

        self.logger.info(f"[{self.bs.time()}] +++++ Starting cluster {job.id}")
        job.allocation = allocation
        job.start_time = self.bs.time()
        self.jobs_to_execute.append(job)


    def startInstance(self, qm, job):
        '''
        If an instance was reserved on the qmobo, reject it back to the QNode sched.
        If an instance is running on the qmobo, kill it (the check of priorities has been made before).
        If the qmobo was OFF, change state to RUNXXX.
        The choice of the pstate will be made by the frequency regulator at the end of the scheduling phase
        '''
        if qm.reserved_job != -1:
            # An instance was reserved, reject it back to the QNode sched
            self.rejectReservedInstance(qm)

        if qm.running_job != -1:
            # A job is running
            self.logger.debug("[{}]------- Mobo {} killed Job {} because another instance arrived".format(self.bs.time(), qm.name, qm.running_job.id))
            old_job = qm.pop_job()

            self.jobs_to_kill.append(old_job)
            self.burning_jobs.discard(old_job)

        job.allocation = ProcSet(qm.batid)
        qm.push_job(job)
        self.jobs_to_execute.append(job)


    def reserveInstance(self, qm, job):
        '''
        If an instance is already reserved on the qmobo, reject it and remove the entry in the dict of reservations.
        Then reserve the qmobo for the job given in argument.
        '''
        if qm.reserved_job != -1:
            # An instance was reserved, reject it back to the QNode sched
            self.rejectReservedInstance(qm)

        self.dict_reserved_jobs_mobos[job.qtask_id].append((qm, job))
        qm.reserved_job = job


    def rejectReservedInstance(self, qm):
        old_job = qm.reserved_job
        qm.reserved_job = -1

        sub_qtask = self.dict_subqtasks[old_job.qtask_id]
        if sub_qtask.is_cluster():
            # Need to remove all reservations
            # TODO verify if all this works...
            # TODO as we do not have LOW priority cluster tasks for now it should no cause problem
            del self.dict_reserved_jobs_mobos[old_job.qtask_id]

        else:
            self.dict_reserved_jobs_mobos[old_job.qtask_id].remove((qm, old_job))

        sub_qtask.reject_reserved_instance(old_job)

        self.qn.onQBoxRejectedInstances([old_job], self.name)
        self.logger.info("[{}]------- Mobo {} rejected Job {} because another instance of higher priority needed it".format(self.bs.time(), qm.name, old_job.id))


    def onJobCompletion(self, job, direct_job = -1):
        '''
        An instance has completed successfully.
        If direct_job is specified, this is a new instance of the same QTask
        that has been dispatched directly.

        If no direct job, check if there are still running instances of the QTask on this QBox and maybe clean the SubQTask.
        '''
        # Retrieve the sub_qtask and the mobo
        sub_qtask = self.dict_subqtasks[job.qtask_id]
        qm = self.dict_ids[job.allocation[0]].dict_mobos[job.allocation[0]]

        assert job in sub_qtask.running_instances, "Job {} was not in the list of running instances of SubQTask {}".format(job.id, sub_qtask.id)

        sub_qtask.instance_finished(job)
        if direct_job != -1:
            # Another instance of the same qtask has to be started immediately
            assert direct_job not in sub_qtask.running_instances and direct_job not in sub_qtask.waiting_instances, "Direct dispatch of instance {} already received/running in {}.".format(direct_job.id, self.name)
            direct_job.allocation = ProcSet(qm.batid)
            assert direct_job.allocation == job.allocation
            qm.push_direct_job(direct_job)
            sub_qtask.mark_running_instance(direct_job)
            self.jobs_to_execute.append(direct_job)
        else:
            self.logger.info("[{}] {} just completed with job_state {} on alloc {} on mobo {} {} with pstate {}".format(self.bs.time(), job.id, job.job_state, str(job.allocation), qm.name, qm.batid, qm.pstate))
            for batid in job.allocation:
                qm = self.dict_ids[batid].dict_mobos[batid]
                qm.pop_job()
            self.checkCleanSubQTask(sub_qtask)


    def onJobKilled(self, job):
        '''
        This instance was killed during the updateAndReportState because the QRad was too hot for its priority
        Or the job was preempted by an instance of higher priority.

        In either cases, need to check if it was the last instance of a QTask and cleanup if need be.
        '''
        sub_qtask = self.dict_subqtasks[job.qtask_id]
        sub_qtask.instance_finished(job)
        self.checkCleanSubQTask(sub_qtask)
        #TODO only this?


    def checkCleanSubQTask(self, sub_qtask):
        '''
        Check whether it was the last running of the SubQTask in this QBox.
        If yes, clean the SubQTask and release the hardlinks on the datasets.
        '''

        if len(sub_qtask.running_instances) == 0 and len(sub_qtask.waiting_instances) == 0:
            self.logger.debug("[{}]--- QBox {} executed all dispatched instances of {}, releasing the hardlinks.".format(self.bs.time(), self.name, sub_qtask.id))
            self.storage_controller.onQBoxReleaseHardLinks(self.disk_batid, sub_qtask.id)
            self.logger.debug(f"[{self.bs.time()}] == Del SubQTask {sub_qtask.id} in the dict of qb {self.name}")
            del self.dict_subqtasks[sub_qtask.id]
            if sub_qtask.id in self.dict_reserved_jobs_mobos:
                del self.dict_reserved_jobs_mobos[sub_qtask.id]


    def doFrequencyRegulation(self):
        # TODO Need to check for all mobos if there is one IDLE.
        # If so, put CPU burn if heating required or turn it off and ask for pstate change
        # For mobos that are still computing something, need to check if a change in pstate is needed


        #TODO We also should retrieve the CPU temperature and check if it's lower than 90 degrees, and lower the speed or shut down the CPU if it's above...

        to_execute = set()
        for qr in self.dict_qrads.values():
            start_cpu_burn = self.qn.do_dispatch and (qr.diffTemp >= 1)
            # Don't start cpu_burn jobs if a dispatch has not been done during this scheduling step
            for qm in qr.dict_mobos.values():
                if qm.batid in self.mobosUnavailable:
                    # Put if OFF if it is not already and kill the running job, if any
                    if qm.state != QMoboState.OFF:# and qm.pstate != qm.max_pstate:
                        if qm.running_job != -1:
                            job = qm.pop_job()
                            self.jobs_to_kill.append(job)
                            self.logger.debug(f"[{self.bs.time()}] Killing {job.id} on {qm.name} ({qm.batid}) because mobo made unavailable")
                        qm.turn_off()
                        self.stateChanges[qm.max_pstate].insert(qm.batid)

                elif start_cpu_burn and (qm.state <= QMoboState.IDLE):
                    # If the mobo is IDLE/OFF and heating is required, start a cpu_burn job
                    jid = "dyn-burn!" + str(self.qn.next_burn_job_id)
                    self.qn.next_burn_job_id += 1
                    burn_job = self.bs.register_job(jid, 1, -1, "burn")
                    burn_job.allocation = ProcSet(qm.batid)
                    burn_job.priority_group = PriorityGroup.BKGD
                    to_execute.add(burn_job)
                    qm.push_burn_job(burn_job)

                    # Then set the pstate of the mobo to 0 (corresponding to full speed)
                    self.stateChanges[qm.min_pstate].insert(qm.batid)
                    #self.logger.debug("++++++++ Change state of {} to {} {} (burn job)".format(qm.batid, 0, qm.pstate))

                elif qm.state == QMoboState.IDLE:
                    # No heating required, turn off this mobo
                    self.logger.debug(f"[{self.bs.time()}] Turning OFF mobo {qm.batid} {qm.name}")
                    qm.turn_off()
                    self.stateChanges[qm.max_pstate].insert(qm.batid)
                    #self.logger.debug("++++++++ Change state of {} to {} {} (off)".format(qm.batid, qm.max_pstate, qm.pstate))

                elif qm.state == QMoboState.LAUNCHING:
                    # A new instance was started during this scheduling step. Update the state and set pstate to qm.min_pstate (max speed)
                    qm.launch_job()
                    self.stateChanges[qm.min_pstate].insert(qm.batid)
                    #self.logger.debug("++++++++ Change state of {} to {} (launch job)".format(qm.batid, qm.pstate))

                elif qm.state >= QMoboState.RUNLOW:
                    # Check if we can increase/decrease the processor speed
                    if qr.diffTemp >= 1 and qm.pstate > qm.min_pstate:
                        # Increase speed
                        qm.pstate -= 1
                        self.stateChanges[qm.pstate].insert(qm.batid)
                        self.logger.debug(f"[{self.bs.time()}] ++++++++ Change state of {qm.batid} to {qm.pstate} (increase speed)")

                    elif qr.diffTemp <= -1 and qm.pstate < (qm.max_pstate-1):
                        # Decrease speed
                        qm.pstate += 1
                        self.stateChanges[qm.pstate].insert(qm.batid)
                        self.logger.debug(f"[{self.bs.time()}] ++++++++ Change state of {qm.batid} to {qm.pstate} (decrease speed)".format(qm.batid, qm.pstate))
                    # Else we stay in this pstate

                else:
                    # Mobo should be in state OFF or RUNBKGD and we have nothing to do
                    assert (qm.state == QMoboState.OFF) or (qm.state == QMoboState.RUNBKGD), "In Frequency regulator, this assert should not be broken (qm.state {} and qr.diffTemp {}".format(qm.state, qr.diffTemp)

                if qm.state == QMoboState.IDLE:
                    assert False, "IDLE mobo {} {} at the end of frequency regulation. Should not happen!".format(qm.batid, qm.name)
                elif qm.pstate == qm.max_pstate and qm.state != QMoboState.OFF:
                    assert False, "Pstate max but the mobo {} {} is not OFF (in state {}). Should not happen!".format(qm.batid, qm.name, qm.state)
                elif qm.state > QMoboState.OFF and qm.pstate == qm.max_pstate:
                    assert False, "Pstate max for running mobo {} {} (in state {}). Should not happen!".format(qm.batid, qm.name, qm.state)

        if len(to_execute) > 0:
            self.logger.info("[{}]--- FrequencyRegulator of {} has started {} burn_jobs".format(self.bs.time(), self.name, len(to_execute)))
            self.jobs_to_execute.extend(to_execute)
            self.burning_jobs.update(to_execute)
