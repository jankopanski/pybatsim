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


#TODO when an event of type "machine_unavailable" is received
# we should mark the qrad/mobos as unavailable as well 

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
        self.site = site                                # Location of the QBox, either "paris" or "bordeaux"

        self.dict_qrads = {}     # Maps the qrad_names to QRad object
        self.dict_ids = {}       # Maps the batids of the mobos to the QRad object that contains it

        # Global counts of all mobos under my watch
        self.mobosAvailable = ProcSet()       # Whether the mobos are available or not (from the QRad hotness and external events point of view)
        self.mobosUnavailable = ProcSet()     # Mobos unavailable due to the QRad being too warm (or from external events)
        #self.mobosRunning = ProcSet()         # List of mobos that are computing an instance. TODO maybe we need to keep track of that list with BKGD/LOW/HIGH

        # TODO maybe we can directly use lists of QRads, may be easier to sort by temperature and get the coolest/warmest when scheduling an instance
        self.availBkgd = ProcSet()
        self.availLow = ProcSet()
        self.availHigh = ProcSet()

        for qr_name, mobos_list in dict_qrads.items():
            qr = QRad(qr_name, bs)
            self.dict_qrads[qr_name] = qr

            # Get properties of the first mobo, since all mobos should be identical
            properties = mobos_list[0][2]
            watts = (properties["watt_per_state"]).split(', ')
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

        # Tells the StorageController who we are and retrieve the batid of our disk
        self.disk_batid = self.storage_controller.onQBoxRegistration(self.name, self)

        self.logger.info("[{}]--- QBox {} initialization completed. Night gathers, and now my watch on {} Qrads and {} QMobos begins!".format(self.bs.time(),self.name, len(self.dict_qrads.keys()), self.nb_mobos))

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

    '''def onOutsideTemperatureChanged(self, new_temperature):
        pass # This is not used by the qarnot schedulers
    '''


    def onNotifyMachineUnavailable(self, machine_batid):
        # The QRad became too hot (from external event), need to kill the instance running on it, if any
        # Then mark this machine as unavailable
        pass

    def onNotifyMachineAvailable(self, machine_batid):
        # Put the machine back available
        pass


    def killOrRejectAllJobs(self):
        '''
        'stop_simulation' event has been received. Returns the lists of jobs to reject and to kill.
        '''
        to_reject = []
        to_kill = list(self.burning_jobs)
        to_kill.extend(self.jobs_to_kill) # There may be jobs being killed during the UpdateAndReportState

        for sub_qtask in self.dict_subqtasks.values():
            to_reject.extend(sub_qtask.waiting_instances)
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

            elif  qr.diffTemp < -3:
                # QRad is too hot to run LOW and BKGD, kill instances if any
                for qm in qr.dict_mobos.values():
                    if qm.state == QMoboState.RUNLOW or qm.state == QMoboState.RUNBKGD:
                        job = qm.pop_job()
                        jobs_to_kill.add(job)

            elif qr.diffTemp < -1:
                # QRad is too hot to run BKGD, kill those CPU burns!
                for qm in qr.dict_mobos.values():
                    if qm.state == QMoboState.RUNBKGD:
                        job = qm.pop_job()
                        jobs_to_kill.add(job)

            for qmobo in qr.dict_mobos.values():
                if (qr.diffTemp >= 1) and (qmobo.state < QMoboState.RUNBKGD):
                    # This mobo is available for BKGD instances
                    self.availBkgd.insert(qmobo.batid)
                elif (qr.diffTemp >= -1) and (qmobo.state < QMoboState.RUNLOW):
                    # This mobo is available for LOW instances
                    self.availLow.insert(qmobo.batid)
                elif (qr.diffTemp >= -4) and (qmobo.state < QMoboState.RUNHIGH):
                    # This mobo is available for HIGH
                    self.availHigh.insert(qmobo.batid)


        if len(jobs_to_kill) > 0:
            #self.logger.info("[{}]--- {} asked to kill the following jobs during updateAndReportState: {}".format(self.bs.time(), self.name, jobs_to_kill))
            self.logger.info("[{}]--- {} asked to kill {} jobs during updateAndReportState".format(self.bs.time(), self.name, len(jobs_to_kill)))
            #for qr in self.dict_qrads.values():
            #    self.logger.info("[{}]----- QRad {} {} target: {}, air: {}, diff: {}".format(self.bs.time(), qr.name, qr.temperature_master, qr.targetTemp, self.bs.air_temperatures[str(qr.temperature_master)], qr.diffTemp))
            self.jobs_to_kill.extend(jobs_to_kill)
            self.burning_jobs.difference_update(jobs_to_kill)
            #self.burning_jobs -= jobs_to_kill

        # Filter out mobos that are marked as unavailable
        self.availBkgd -= self.mobosUnavailable
        self.availLow -= self.mobosUnavailable
        self.availHigh -= self.mobosUnavailable


        self.logger.info("[{}]--- {} reporting the available slots for bkgd/low/high: {}/{}/{}".format(self.bs.time(), self.name, len(self.availBkgd), len(self.availLow), len(self.availHigh)))
        return [self.name, len(self.availBkgd), len(self.availLow), len(self.availHigh)]


    def onDispatchedInstance(self, instances, priority_group, qtask_id):
        '''
        Instances is a list of Batsim jobs corresponding to the instances dispatched to this QBox.
        Priority_group is either bkgd/low/high and tells in which list of available
        mobos we should execute the instances.

        Datasets are shared between the instances of the same QTask. So we only need to retrive the datasets once for all instances
        '''
        self.logger.info("[{}]--- {} received {} instances of {} for the priority group {}".format(self.bs.time(), self.name, len(instances), qtask_id, priority_group))
        if qtask_id in self.dict_subqtasks:
            # Some instances of this QTask have already been received by this QBox
            sub_qtask = self.dict_subqtasks[qtask_id]
            sub_qtask.waiting_instances.extend(instances.copy()) #TODO maybe don't need this copy since we do extend
            if len(sub_qtask.waiting_datasets) == 0:
                self.scheduleInstances(sub_qtask)
        else:
            # This is a QTask "unknown" to the QBox.
            # Create and add the SubQTask to the dict
            list_datasets = self.bs.profiles[instances[0].workload][instances[0].profile]["datasets"]
            if list_datasets is None:
                list_datasets = []

            sub_qtask = SubQTask(qtask_id, priority_group, instances.copy(), list_datasets)
            self.dict_subqtasks[qtask_id] = sub_qtask

            # Then ask for the data staging of the required datasets
            for dataset_id in list_datasets:
                if self.storage_controller.onQBoxAskDataset(self.disk_batid, dataset_id):
                    # The dataset is already on disk, ask for a hardlink
                    self.storage_controller.onQBoxAskHardLink(self.disk_batid, dataset_id, sub_qtask.id)
                else:
                    # The dataset is not on disk yet, put it in the lists of waiting datasets
                    sub_qtask.waiting_datasets.append(dataset_id)
                    self.waiting_datasets.append(dataset_id)

            # If all required datasets are on disk, launch the instances
            if len(sub_qtask.waiting_datasets) == 0:
                self.scheduleInstances(sub_qtask)


    def onDatasetArrivedOnDisk(self, dataset_id):
        '''
        The Storage Controller notifies that the required dataset arrived on the disk.
        Ask for a hard link on this dataset if there are tasks that were waiting for this dataset.
        Then check if we can launch instances.
        '''
        to_launch = []
        if dataset_id in self.waiting_datasets:
            n = self.waiting_datasets.count(dataset_id)
            self.logger.info("[{}]--- Dataset {} arrived on QBox {} and was waited by {} SubQTasks".format(self.bs.time(), dataset_id, self.name, n))
            for sub_qtask in self.dict_subqtasks.values():
                if dataset_id in sub_qtask.waiting_datasets:
                    sub_qtask.waiting_datasets.remove(dataset_id)
                    self.waiting_datasets.remove(dataset_id)
                    self.storage_controller.onQBoxAskHardLink(self.disk_batid, dataset_id, sub_qtask.id)
                    if len(sub_qtask.waiting_datasets) == 0:
                        # The SubQTask has all the datasets, launch it
                        to_launch.append(sub_qtask)

                    n-= 1
                    if n == 0:
                        # All SubQTasks waiting for this dataset were found, stop
                        assert dataset_id not in self.waiting_datasets # TODO remove this at some point?
                        break

            # For each SubQTask from the highest priority, launch the instances
            for sub_qtask in sorted(to_launch, key=lambda qtask:-qtask.priority_group):
                self.scheduleInstances(sub_qtask)
        #else
        # The dataset is no longer required by a QTask. Do nothing


    def scheduleInstances(self, sub_qtask):
        '''
        All datasets required by this sub_qtask are on disk and hard links were already requested.
        Execute an instance HIGH on the coolest QRad (if possible without preempting LOW instance, don't care about BKGD)
        Execute an instance BKGD/LOW on the warmest QRad (preempt BKGD task if any)
        '''
        n = len(sub_qtask.waiting_instances)
        if sub_qtask.priority_group == PriorityGroup.HIGH:
            # Find coolest QRad which is not running LOW instance, i.e. the QRad with the greatest diffTemp
            running_low = [] # List of mobos that are running LOW instances
            available_slots = self.availBkgd | self.availLow | self.availHigh # Get all mobos on which we can start a HIGH instance
            qr_list = sorted(self.dict_qrads.values(), key=lambda qr:-qr.diffTemp) # Take QRads by decreasing temperature difference (i.e., increasing heating capacity)
            for qr in qr_list:
                for batid in (qr.pset_mobos & available_slots):
                    if qr.dict_mobos[batid].state <= QMoboState.RUNBKGD: # Either OFF/IDLE or running BKGD
                        job = sub_qtask.pop_waiting_instance()
                        sub_qtask.mark_running_instance(job)
                        self.startInstance(qr.dict_mobos[batid], job)
                        n-=1
                        if n == 0:
                            return # All instances have been started
                    elif qr.dict_mobos[batid].state == QMoboState.RUNLOW:
                        running_low.append(batid)
            # There are still instances to start, take mobos that are running LOW instances.
            for batid in running_low: # Mobos in this list are already sorted by coolest QRad first
                job = sub_qtask.pop_waiting_instance()
                sub_qtask.mark_running_instance(job)
                self.startInstance(self.dict_ids[batid].dict_mobos[batid], job)
                n-=1
                if n == 0:
                    return # All instances have been started

        else: # This is a LOW instance
            # Find warmest QRad among the availLow and availBkgd
            available_slots = self.availBkgd | self.availLow # Get all mobos in which we can start a LOW instance
            qr_list = sorted(self.dict_qrads.values(), key=lambda qr:qr.diffTemp) # Take QRads by increasing temperature difference (i.e., decreasing heat capacity)
            for qr in qr_list:
                for batid in (qr.pset_mobos & available_slots):
                    if qr.dict_mobos[batid].state <= QMoboState.RUNBKGD: # Either OFF/IDLE or running BKGD
                        job = sub_qtask.pop_waiting_instance()
                        sub_qtask.mark_running_instance(job)
                        self.startInstance(qr.dict_mobos[batid], job)
                        n-=1
                        if n == 0:
                            return # All instances have been started

        # Some instances were dispatched by cannot be started yet, return them to the QNode
        self.logger.info("[{}]--- {} still has {} instances of {} to start, rejecting these instances back to the QNode but this should not happen.".format(self.bs.time(), self.name, len(sub_qtask.waiting_instances), sub_qtask.id))
        self.logger.info("[{}]--- {} has available slots for bkgd/low/high: {}/{}/{}".format(self.bs.time(), self.name, len(self.availBkgd), len(self.availLow), len(self.availHigh)))

        assert len(sub_qtask.waiting_instances) > 0, "QBox wants to reject 0 instances to the QNode..."
        self.qn.onQBoxRejectedInstances(sub_qtask.waiting_instances.copy(), self.name) # TODO maybe we don't need to copy this
        sub_qtask.waiting_instances = []


    def startInstance(self, qm, job):
        '''
        If an instance is running on the qmobo, kill it (the check of priorities has been made before).
        If the qmobo was OFF, change state to RUNXXX.
        The choice of the pstate will be made by the frequency regulator at the end of the scheduling phase
        '''
        if qm.running_job != -1:
            # A job is running
            self.logger.debug("[{}]------- Mobo {} killed Job {} because another instance arrived".format(self.bs.time(), qm.name, qm.running_job.id))
            old_job = qm.pop_job()

            self.jobs_to_kill.append(old_job)
            self.burning_jobs.discard(old_job)

        job.allocation = ProcSet(qm.batid)
        qm.push_job(job)
        #self.mobosRunning.insert(qm.batid) # TODO May not be useful to track the list of mobos running,
        self.jobs_to_execute.append(job)



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
            self.logger.debug("[{}] {} just completed with job_state {} on alloc {} on mobo {} {}".format(self.bs.time(), job.id, job.job_state, str(job.allocation), qm.name, qm.batid))
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
            del self.dict_subqtasks[sub_qtask.id]


    def doFrequencyRegulation(self):
        # TODO Need to check for all mobos if there is one IDLE.
        # If so, put CPU burn if heating required or turn it off and ask for pstate change
        # For mobos that are still computing something, need to check if a change in pstate is needed
        to_execute = set()
        for qr in self.dict_qrads.values():
            start_cpu_burn = self.qn.do_dispatch and (qr.diffTemp >= 1)
            # Don't start cpu_burn jobs if a dispatch has not been done during this scheduling step
            for qm in qr.dict_mobos.values():
                if start_cpu_burn and (qm.state <= QMoboState.IDLE):
                    # If the mobo is IDLE/OFF and heating is required, start a cpu_burn job
                    jid = "dyn-burn!" + str(self.qn.next_burn_job_id)
                    self.qn.next_burn_job_id += 1
                    self.bs.register_job(jid, 1, -1, "burn")
                    burn_job = Job(jid, 0, -1, 1, "", "")
                    burn_job.allocation = ProcSet(qm.batid)
                    burn_job.priority_group = PriorityGroup.BKGD
                    to_execute.add(burn_job)
                    qm.push_burn_job(burn_job)

                    # Then set the pstate of the mobo to 0 (corresponding to full speed)
                    self.stateChanges[0].insert(qm.batid)

                elif qm.state == QMoboState.IDLE:
                    # No heating required, turn off this mobo
                    qm.turn_off()
                    self.stateChanges[qm.max_pstate].insert(qm.batid)

                elif qm.state >= QMoboState.RUNLOW:
                    # Check if we can increase/decrease the processor speed
                    if qr.diffTemp >= 1 and qm.pstate > 0:
                        # Increase speed
                        qm.pstate -= 1
                        self.stateChanges[qm.pstate].insert(qm.batid)

                    elif qr.diffTemp <= -1 and qm.pstate < (qm.max_pstate-1):
                        # Decrease speed
                        qm.pstate += 1
                        self.stateChanges[qm.pstate].insert(qm.batid)
                    # Else we stay in this pstate

                else:
                    # Mobo should be in state OFF or RUNBKGD and we have nothing to do
                    assert (qm.state == QMoboState.OFF) or (qm.state == QMoboState.RUNBKGD), "In Frequency regulator, this assert should not be broken (qm.state {} and qr.diffTemp {}".format(qm.state, qr.diffTemp)

                if qm.state == QMoboState.IDLE:
                    assert False, "IDLE mobo {} at the end of frequency regulation. Should not happen!".format(qm.name)
                elif qm.state > QMoboState.OFF and qm.pstate == qm.max_pstate:
                    assert False, "Pstate max for running mobo {}. Should not happen!".format(qm.name)

        if len(to_execute) > 0:
            self.logger.info("[{}]--- FrequencyRegulator of {} has started {} burn_jobs".format(self.bs.time(), self.name, len(to_execute)))
            self.jobs_to_execute.extend(to_execute)
            self.burning_jobs.update(to_execute)
