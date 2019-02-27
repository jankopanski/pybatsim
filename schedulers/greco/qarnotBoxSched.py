from batsim.batsim import Batsim, Job
from qarnotUtils import QTask, FrequencyRegulator

from procset import ProcSet
from collections import defaultdict

import logging


'''
This is the qarnot QBox scheduler


List of available mobos:
- AvailForBkgd: for each QBox number of mobos that are too cold and are running CPU burn tasks to heat
  We start CPU burn if air_temp < target - 1
  We stop the CPU burn if air_temp > target + 1
  We can start low and high priority tasks here as well

- AvailForLow: for each QBox number of mobos that are too hot to run cpu burn and are probably idle (or running cpu burn)
  We can start low priority tasks if air_temp < target + 1
  We stop the low priority tasks there if air_temp > target + 3
  We can start high priority tasks here as well

- AvailForHigh: for each QBox number of mobos that are too hot to run low priority tasks
  We can start high priority tasks if air_temp < target + 4
  We stop high priority tasks if air_temp > target + 10


When a task of higher priority is sent to a mobo that is already running something,
wait for all the datasets to arrive before stopping the execution of the current task.


#TODO when an event of type "machine_unavailable" is received
# we should mark the qrad/mobos as unavailable as well 

#TODO need to add a "warmup" time for booting the mobo when a new task is executed on it

'''

class QarnotBoxSched():
    def __init__(self, name, list_mobos, bs, qn, storage_controller):
        ''' WARNING!!!
        The init of the QBox Schedulers is done upon receiving
        the SimulationBegins in the QNode Scheduler 
        Thus the init of QBox and onSimulationBegins of QBox is quite the same
        '''
        self.bs = bs
        self.qn = qn
        self.storage_controller = storage_controller
        self.logger = bs.logger
        self.name = name

        self.dict_mobos = {} # Maps the batsim ids of the mobos to the properties

        for (index, properties) in list_mobos:
            watts = (properties["watt_per_state"]).split(', ')
            properties["nb_pstates"] = len(watts)
            properties["watt_per_state"] = [float((x.split(':'))[-1]) for x in watts]
            self.dict_mobos[index] = properties
        self.nb_mobos = len(self.dict_mobos)

        self.availBkgd = ProcSet()
        self.availLow = ProcSet()
        self.availHigh = ProcSet()

        self.targetTemp = {}
        self.diffTemp = {}

        # Tells the StorageController who we are
        self.storage_controller.onQBoxRegistration(self.name, self)

        self.logger.info("--- QBox {} initialization completed. Night gathers, and now my watch on {} mobos begins!".format(self.name, self.nb_mobos))


    def onSimulationEnds(self):
        pass

    def onBeforeEvents(self):
        pass

    def onNoMoreEvents(self):
        pass

    def onTargetTemperatureChanged(machine_id, new_temperature):
        #self.diffTemp[machine_id] = self.diffTemp[machine_id] + self.targetTemp[machine_id] - new_temperature
        self.diffTemp[machine_id] = self.bs.air_temeratures[machine_id] - new_temperature
        self.targetTemp[machine_id] = new_temperature


    def onNotifyMachineUnavailable(machine_id):
        # The QRad became too hot, need to kill the instance running on it, if any
        # Then mark this machine as unavailable
        pass

    def onNotifyMachineAvailable(machine_id):
        # Put the machine back available
        pass


    def updateAndReportState(self):
        '''
        The state of the QBox is updated every 30 seconds.
        The temperature of the QRads is checked and decisions are taken:
         - Whether to kill an instance if the rad is too hot
         - Whether to change the frequencies of the mobos
        Then, the list of mobos available for each priority group is updated
        and returned back to the QNode.
        Returns a list [qbox_id, slots bkgd, slots low, slots high]
        '''
        pass

    def onDispatchedInstance(self, instances, priority_group):
        '''
        Instances is a list of Batsim jobs corresponding to the instances dispatched to this QBox.
        Priority_group is either bkgd/low/high and tells in which list of available
        mobos we should execute the instances.

        Datasets are shared between the instances of the same QTask. So we only need to retrive the datasets once for all instances
        WARNING!!! The list of datasets in a job profile can be 'null'

        Execute an instance HIGH on the coolest QRad (if possible without preempting LOW instance, don't care about BKGD)
        Execute an instance BKGD/LOW on the warmest QRad (preempt BKGD task if any)

        WARNING!!! Only kill the already running instance once the datasets have arrived.
        '''
        pass

    def onJobCompletion(self, job, direct_job = -1):
        '''
        An instance has completed successfully.
        If direct_job is specified, this is a new instance of the same QTask
        that has been dispatched directly.
        '''
        pass


    def onJobKilled(self, job):
        pass #TODO pass?


    def onDatasetArrived(self, dataset_id):
        '''
        A datastaging job has finished, check if we can launch instances.
        If instances are launched on some mobos, kill already running instances before.
        '''
