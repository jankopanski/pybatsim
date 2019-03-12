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
    def __init__(self, name, dict_qrads, bs, qn, storage_controller):
        ''' WARNING!!!
        The init of the QBox Schedulers is done upon receiving
        the SimulationBegins in the QNode Scheduler 
        Thus there is no onSimulationBegins called for a QBox sched
        '''
        self.bs = bs
        self.qn = qn
        self.storage_controller = storage_controller
        self.logger = bs.logger
        self.name = name

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
            watts = (properties["watt_per_state"]).split(', ')
            properties["watt_per_state"] = [float((x.split(':'))[-1]) for x in watts]
            properties["nb_pstates"] = len(watts)
            qr.properties = properties

            # Create the QMobos
            max_pstate = properties["nb_pstates"]-1
            dict_mobos = {}
            for (batid, mobo_name,_) in mobos_list:
                dict_mobos[batid] = QMobo(mobo_name, batid, max_pstate)
                self.dict_ids[batid] = qr

            qr.dict_mobos = dict_mobos
            qr.pset_mobos = ProcSet(*dict_mobos.keys())

        # Assume all mobos are available for LOW tasks at the beginning (and not BKGD to not start a cpu burn on every resource at t=0)
        self.mobosAvailable = ProcSet(*self.dict_ids.keys())
        self.availLow = ProcSet(*self.dict_ids.keys())
        self.nb_mobos = len(self.dict_ids)

        self.stateChanges = defaultdict(ProcSet) # Keys are target pstate, values are list of resources for which we change the state

        self.dict_subqtasks = {} # Maps the QTask id to the SubQTask object
        self.waiting_datasets = [] # List of datasets for which data staging has been asked
                                   # If a dataset appears multiple times in the list, it means that multiple QTasks are waiting for this dataset

        # Tells the StorageController who we are and retrieve the batid of our disk
        self.disk_batid = self.storage_controller.onQBoxRegistration(self.name, self)

        self.logger.info("[{}]--- QBox {} initialization completed. Night gathers, and now my watch on {} mobos begins!".format(self.bs.time(),self.name, self.nb_mobos))


    def onSimulationEnds(self):
        pass

    def onBeforeEvents(self):
        self.stateChanges.clear()
        self.jobs_to_execute = []
        self.jobs_to_kill = []

    def onNoMoreEvents(self):
        self.doFrequencyRegulation()

        if len(self.jobs_to_kill) > 0:
            self.bs.kill_jobs(self.jobs_to_kill)
        if len(self.jobs_to_execute) > 0:
            self.bs.execute_jobs(self.jobs_to_execute)

        # Then append all SET_RESOURCE_STATE events that occured during this scheduling phase
        for pstate, resources in self.stateChanges.items():
            self.bs.set_resource_state(resources, pstate)

    def onTargetTemperatureChanged(self, machine_batid, new_temperature):
        self.dict_ids[machine_batid].diffTemp = new_temperature - self.bs.air_temperatures[str(machine_batid)]
        self.dict_ids[machine_batid].targetTemp = new_temperature


    def onNotifyMachineUnavailable(self, machine_batid):
        # The QRad became too hot (from external event), need to kill the instance running on it, if any
        # Then mark this machine as unavailable
        pass

    def onNotifyMachineAvailable(self, machine_batid):
        # Put the machine back available
        pass


    def updateAndReportState(self):
        '''
        The state of the QBox is updated every 30 seconds.
        The temperature of the QRads is checked and decisions are taken to kill an instance if the rad is too hot
        Then, the list of mobos available for each priority group is updated
        and returned back to the QNode.
        The frequency regulation will be done when all events are treated, called in onNoMoreEvents
        Returns a list [qbox_name, slots bkgd, slots low, slots high]
        '''
        self.availBkgd.clear()
        self.availLow.clear()
        self.availHigh.clear()

        jobs_to_kill = []
        for qr in self.dict_qrads.values():
            qr.diffTemp = qr.targetTemp - self.bs.air_temperatures[str(qr.pset_mobos[0])]
            self.logger.info("[{}]----- QRad {} target: {}, air: {}, diff: {}".format(self.bs.time(),qr.name, qr.targetTemp, self.bs.air_temperatures[str(qr.pset_mobos[0])], qr.diffTemp))
            # Check if we have to kill instances
            if qr.diffTemp < -10:
                # QRad is too hot to run HIGH LOW and BKGD, gotta kill'em all!
                for qm in qr.dict_mobos.values():
                    if qm.running_job != -1:
                        job = qm.pop_job()
                        jobs_to_kill.append(job)

            elif  qr.diffTemp < -3:
                # QRad is too hot to run LOW and BKGD, kill instances if any
                for qm in qr.dict_mobos.values():
                    if qm.state == QMoboState.RUNLOW or qm.state == QMoboState.RUNBKGD:
                        job = qm.pop_job()
                        jobs_to_kill.append(job)

            elif qr.diffTemp < -1:
                # QRad is too hot to run BKGD, kill those CPU burns!
                for qm in qr.dict_mobos.values():
                    if qm.state == QMoboState.RUNBKGD:
                        job = qm.pop_job()
                        jobs_to_kill.append(job)


            #self.logger.info("[{}]--- QBox {} reporting  BEFORE UPDATE diffTMP -Unavailable the available slots for bkgd/low/high: {}/{}/{}".format(self.bs.time(),self.name,len(self.availBkgd), len(self.availLow), len(self.availHigh)))

            # Update the list of available mobos
            # Update the list of available mobos
            if qr.diffTemp >= 1:
                self.availBkgd |= ProcSet(*qr.dict_mobos.keys())
            elif qr.diffTemp >= -1:
                for qmobo in qr.dict_mobos.values():
                    if qmobo.state < QMoboState.RUNLOW: # This mobo is available for LOW instances
                        self.availLow.insert(qmobo.batid)
                #self.availLow |= ProcSet(*qr.dict_mobos.keys())
            elif qr.diffTemp >= -4:
                for qmobo in qr.dict_mobos.values():
                    if qmobo.state < QMoboState.RUNHIGH: # This mobo is available for HIGH
                        self.availHigh.insert(qmobo.batid)
                    #self.availHigh |= ProcSet(*qr.dict_mobos.keys())
            #self.logger.info("[{}]--- QBox {} reporting  AFTER  UPDATE diffTMP -Unavailable the available slots for bkgd/low/high: {}/{}/{}".format(self.bs.time(),self.name,len(self.availBkgd), len(self.availLow), len(self.availHigh)))


        if len(jobs_to_kill) > 0:
            self.logger.info("[{}]--- QBox {} asked to kill the following jobs during updateAndReportState: {}".format(self.bs.time(),self.name, jobs_to_kill))
            self.jobs_to_kill.extend(jobs_to_kill)

        #self.logger.info("[{}]--- QBox {} reporting  before -Unavailable the available slots for bkgd/low/high: {}/{}/{}".format(self.bs.time(),self.name,len(self.availBkgd), len(self.availLow), len(self.availHigh)))

        # Filter out mobos that are marked as unavailable
        self.availBkgd -= self.mobosUnavailable
        self.availLow -= self.mobosUnavailable
        self.availHigh -= self.mobosUnavailable

        self.logger.info("[{}]--- QBox {} reporting the available slots for bkgd/low/high: {}/{}/{}".format(self.bs.time(),self.name,len(self.availBkgd), len(self.availLow), len(self.availHigh)))
        return [self.name, len(self.availBkgd), len(self.availLow), len(self.availHigh)]


    def onDispatchedInstance(self, instances, priority_group, qtask_id):
        '''
        Instances is a list of Batsim jobs corresponding to the instances dispatched to this QBox.
        Priority_group is either bkgd/low/high and tells in which list of available
        mobos we should execute the instances.

        Datasets are shared between the instances of the same QTask. So we only need to retrive the datasets once for all instances
        '''
        self.logger.info("[{}]--- QBox {} received {} instances of {} for the priority group {}".format(self.bs.time(), self.name, len(instances), qtask_id, priority_group))
        if qtask_id in self.dict_subqtasks:
            # Some instances of this QTask have already been received by this QBox
            qtask = self.dict_subqtasks[qtask_id]
            qtask.waiting_instances.extend(instances)
            if len(qtask.waiting_datasets) == 0:
                self.scheduleInstances(qtask)
        else:
            # This is a QTask "unknown" to the QBox.
            # Create and add the SubQTask to the dict
            qtask = SubQTask(qtask_id, priority_group, instances.copy())
            self.dict_subqtasks[qtask_id] = qtask
            # Then ask for the data staging of the required datasets
            for dataset_id in qtask.datasets:
                if self.storage_controller.onQBoxAskDataset(self.disk_batid, dataset_id):
                    # The dataset is already on disk, ask for a hardlink
                    self.storage_controller.onQBoxAskHardLink(self.disk_batid, dataset_id)
                else:
                    # The dataset is not on disk yet, put it in the lists of waiting datasets
                    qtask.waiting_datasets.append(dataset_id)
                    self.waiting_datasets.add(dataset_id)

            # If all required datasets are on disk, launch the instances
            if len(qtask.waiting_datasets) == 0:
                self.scheduleInstances(qtask)


    def onDatasetArrivedOnDisk(self, dataset_id):
        '''
        The Storage Controller notifies that the required dataset arrived on the disk.
        Ask for a hard link on this dataset if there are tasks that were waiting for this dataset.
        Then check if we can launch instances.
        '''
        to_launch = []
        if dataset_id in self.waiting.datasets:
            n = self.waiting_datasets.count(dataset_id)
            self.logger.info("[{}]--- Dataset {} arrived on QBox {} and was waited by {} SubQTasks".format(self.bs.time(),dataset_id, self.name, n))
            for qtask in self.dict_subqtasks.values():
                if dataset_id in qtask.waiting_datasets:
                    qtask.waiting_datasets.remove(dataset_id)
                    self.waiting_datasets.remove(dataset_id)
                    self.storage_controller.onQBoxAskHardLink(self.disk_batid, dataset_id)
                    if len(qtask.waiting_datasets) == 0:
                        # The SubQTask has all the datasets, launch it
                        to_launch.append(qtask)

                    n-= 1
                    if n == 0:
                        # All SubQTasks waiting for this dataset were found, stop
                        assert dataset_id not in self.waiting_datasets # TODO remove this at some point?
                        break

            # For each SubQTask from the highest priority, launch the instances
            for qtask in sorted(to_launch, key=lambda qtask:-qtask.priority_group):
                self.scheduleInstances(qtask)
        #else
        # The dataset is no longer required by a QTask. Do nothing


    def scheduleInstances(self, qtask):
        '''
        All datasets required by this qtask are on disk and hard links were already requested.
        Execute an instance HIGH on the coolest QRad (if possible without preempting LOW instance, don't care about BKGD)
        Execute an instance BKGD/LOW on the warmest QRad (preempt BKGD task if any)
        '''
        n = len(qtask.waiting_instances)
        if qtask.priority_group == PriorityGroup.HIGH:
            # Find coolest QRad which is not running LOW instance, i.e. the QRad with the greatest diffTemp
            running_low = [] # List of mobos that are running LOW instances
            available_slots = self.availBkgd | self.availLow | self.availHigh # Get all mobos on which we can start a HIGH instance
            qr_list = sorted(self.dict_qrads.values(), key=lambda qr:-qr.diffTemp) # Take QRads by decreasing temperature difference (i.e., increasing heating capacity)
            for qr in qr_list:
                for batid in (qr.pset_mobos & available_slots):
                    if qr.dict_mobos[batid].state <= QMoboState.RUNBKGD: # Either OFF/IDLE or running BKGD
                        job = qtask.waiting_instances.pop()
                        qtask.running_instances.append(job)
                        self.startInstance(qr.dict_mobos[batid], job)
                        n-=1
                        if n == 0:
                            return # All instances have been started
                    elif qr.dict_mobos[batid].state == QMoboState.RUNLOW:
                        running_low.append(batid)
            # There are still instances to start, take mobos that are running LOW instances.
            for batid in running_low: # Mobos in this list are already sorted by coolest QRad first
                job = qtask.waiting_instances.pop()
                qtask.running_instances.append(job)
                self.startInstance(qr.dict_mobos[batid], job)
                n-=1
                if n == 0:
                    return # All instances have been started

        else: # This is a LOW instance
            # Find warmest QRad among the availLow and availBkgd
            available_slots = self.availBkgd | self.availLow # Get all mobos in which we can start a LOW instance
            qr_list = sorted(self.dict_qrads.values(), key=lambda qr:qr.diffTemp) # Take QRads by increasing temperature difference (i.e., decreasing heat capacity)
            #TODO if ( len(available_slots) < 0 or len(qr_list) < 0)
            for qr in qr_list:
                for batid in (qr.pset_mobos & available_slots):
                    if qr.dict_mobos[batid].state <= QMoboState.RUNBKGD: # Either OFF/IDLE or running BKGD
                        job = qtask.waiting_instances.pop()
                        qtask.running_instances.append(job)
                        self.startInstance(qr.dict_mobos[batid], job)
                        n-=1
                        if n == 0:
                            return # All instances have been started
        self.logger.info("[{}]--- QBox {} still have {} instances of {} to start, this should not happen.".format(self.bs.time(),self.name, len(qtask.waiting_instances), qtask.id))
        self.qn.onRejectedInstance(qtask.waiting_instances.copy()) #assert False
        #qtask.waiting_instances
        #TODO need to reject the remaining instances of the SubQTask?


    def startInstance(self, qm, job):
        '''
        If an instance is running on the qmobo, kill it (the check of priorities has been made before).
        If the qmobo was OFF, change state to RUNXXX.
        The choice of the pstate will be made by the frequency regulator at the end of the scheduling phase
        '''
        if qm.running_job != -1:
            # A job is running
            self.bs.kill_jobs([qm.running_job])
            self.logger.info("[{}]------- Mobo {} killed Job {} because another instance arrived".format(self.bs.time(),qm.name, qm.running_job.id))

        job.allocation = ProcSet(qm.batid)
        qm.push_job(job)
        self.jobs_to_execute.append(job)



    def onJobCompletion(self, job, direct_job = -1):
        '''
        An instance has completed successfully.
        If direct_job is specified, this is a new instance of the same QTask
        that has been dispatched directly.

        If no direct job, check if there are still running instances of the QTask on this QBox:
          - If no, clean the QTask and release the hardlinks on the datasets
          - If yes, do nothing
        '''
        qm = self.dict_ids[job.allocation[0]].dict_mobos[job.allocation[0]]
        if direct_job != -1:
            # Another instance of the same qtask has to be started immediately
            self.dict_subqtasks[direct_job.qtask_id].running_instances.append(direct_job)
            #self.dict_subqtasks[direct_job.qtask_id].waiting_instances.remove(direct_job)
            qm.push_direct_job(direct_job)
            self.checkCleanSubQTask(direct_job)

        else:
            qm.pop_job()
            self.checkCleanSubQTask(job)

    def onJobKilled(self, job):
        '''
        This instance was killed during the updateAndReportState because the QRad was too hot for its priority
        Or the job was preempted by an instance of higher priority.

        In either cases, need to check if it was the last instance of a QTask and cleanup if need be.
        '''
        self.checkCleanSubQTask(job)
        #TODO only this?


    def checkCleanSubQTask(self, job):
        '''
        An instance just finished, need to check whether it was the last running of the SubQTask in this QBox.
        If yes, clean the SubQTask and release the hardlinks on the datasets.
        '''
        qtask = self.dict_subqtasks[job.qtask_id]
        assert job in qtask.running_instances, "Job {} was not in the list of running instances of SubQTask {}".format(job.id, qtask.id)
        qtask.running_instances.remove(job)

        if len(qtask.running_instances) == 0 and len(qtask.waiting_instances) == 0:
            self.logger.info("[{}]--- QBox {} executed all instances of {}, releasing the hardlinks.".format(self.bs.time(),self.name, qtask.id))
            self.storage_controller.onQBoxReleaseHardLinks(self.disk_batid, qtask.datasets)
            del self.dict_subqtasks[qtask.id]


    def doFrequencyRegulation(self):
        pass
        # TODO Need to check for all mobos if there is one IDLE. If so, turn it off and ask for pstate change
        # For mobos that are still computing something, need to check if a change in pstate is needed

