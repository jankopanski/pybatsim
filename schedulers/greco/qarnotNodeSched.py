from batsim.batsim import BatsimScheduler, Batsim, Job
from StorageController import StorageController
from qarnotBoxSched import QarnotBoxSched

from procset import ProcSet
from collections import defaultdict

import logging

'''
This is the scheduler instanciated by Pybatsim for a simulation of the Qarnot platform and has two roles:
- It does the interface between Batsim/Pybatsim API and the QNode/QBox schedulers (manager)
- It holds the implementation of the Qarnot QNode scheduler (dispatcher)



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


For the dispatch of a task:
Select the task of highest priority that have the smallest number of running instances.
Then to choose the QBoxes/Mobos where to send it:
- First send as much instances as possible on mobos available for bkgd
  (the ones that are running bkgd tasks, because they have the most "coolness reserve"
  and will likely run faster and longer than idle cpus)

- Second on mobos available for low
- Third on mobos available for high

The QBoxes where the instances are sent are sorted by increasing number of available mobos
(so keep large blocks of mobos available for cluster tasks)
Always send as much instances as there are available mobos in a QBox,


A dispatch is done when a task arrives in the system or every 30 seconds.
When an instance finishes, we do a quick dispatch if there are more instances of the same task to execute
(and if there is no other waiting task of higher priority value)

Notion of priority for tasks:
There are 3 major groups: Bkgd, Low and High
Each task has a certain numerical value of priority.

A task cannot preempt a task in the same priority group (even if its numerical priority is smaller)
but can preempt tasks from a lower priority group.


'''

class QarnotNodeSched(BatsimScheduler):
    def __init__(self, options):
        super().__init__(options)

        # For the manager
        self.dict_qboxes = {}    # Maps the QBox id to the QarnotBoxSched object
        self.dict_resources = {} # Maps the Batsim resource id to the QarnotBoxSched object

        # Dispatcher
        self.job_queue = []     # The queue of jobs waiting to be dispatched
        self.job_mapping = {}   # Maps the job id to the qbox id where it has been sent to
        self.qboxes_available_mobos = {} # Maps the QBox id to the number of available qmobos for each type (bkgd, low, high)
                                         # The values should be a triplet
                                         # Values are updated by the QBoxes
                      # TODO maybe change this to a list of 4-uple (qbox id, slots bkgd, slots low, slots high)
                      # This way we can sort the list as we want
                      # BUT BEWARE, NEED TO EMPTY THE LIST AT EACH TIME ...

        #NOT USED AT THE MOMENT:
        #self.qboxes_free_disk_space = {} # Maps the QBox id to the free disk space (in GB)
        #self.qboes_queued_upload_size = {} # Maps the QBox id to the queued upload size (in GB)


    def onSimulationBegins(self):
      assert self.bs.dynamic_job_registration_enabled, "Registration of dynamic jobs should be enabled to use this scheduler"
      assert len(self.bs.air_temperatures) > 0, "Temperature option '-T 1' of Batsim should be set to use this scheduler"
      # TODO maybe change the option to send every 30 seconds instead of every message of Batsim (see TODO list)

      self.initQBoxesAndStorageController()
      for qb in self.dict_qboxes.values():
        qb.onBeforeEvents()
        qb.onSimulationBegins()

      self.storage_controller.onSimulationBegins()

      self.logger.info("- QNode: End of SimulationBegins")


      self.bs.notify_registration_finished()
      #TODO remove this

    def onSimulationEnds(self):
      for qb in self.dict_qboxes.values():
        qb.onSimulationEnds()

      self.storage_controller.onSimulationEnds()


    def initQBoxesAndStorageController(self):
      # Let's create the StorageController
      self.storage_controller = StorageController(self.bs.machines["storage"], self.bs, self)
      

      # Retrieve the QBox ids and the associated list of QMobos Batsim ids
      dict_ids = defaultdict(list) # Maps the QBox name to a list of (batsim_id of the mobo and properties)
      for res in self.bs.machines["compute"]:
        batsim_id = res["id"]
        names = res["name"].split("_") # Name of a resource is of the form "QBOX-XXXX_QRAD-YYYY_qmZ"
        qb_name = names[0]
        qr_name = names[1]
        qm_name = names[2]
        properties = res["properties"]

        dict_ids[qb_name].append((batsim_id, properties))

      # Let's create the QBox Schedulers
      for (qb_name, list_mobos) in dict_ids.items():
        qb = QarnotBoxSched(qb_name, list_mobos, self.bs, self, self.storage_controller)

        self.dict_qboxes[qb_name] = qb

      # Populate the mapping between batsim resource ids and the associated QarnotBoxSched
      for mobo_tuple in list_mobos:
        self.dict_resources[mobo_tuple[0]] = qb

      self.nb_qboxes = len(self.dict_qboxes)
      self.nb_computing_resources = len(self.dict_resources)


    def onBeforeEvents(self):
      for qb in self.dict_qboxes.values():
        qb.onBeforeEvents()

    def onNoMoreEvents(self):
      '''if self.received_job:
        self.doDispatch()
        self.received_job = False'''

      for qb in self.dict_qboxes.values():
        qb.onNoMoreEvents()

    def onJobSubmission(self, job):
      #self.job_queue.append(job)
      self.bs.reject_jobs([job])