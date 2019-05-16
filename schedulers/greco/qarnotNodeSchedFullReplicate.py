from batsim.batsim import BatsimScheduler, Batsim, Job
from qarnotNodeSched import QarnotNodeSched
from StorageController import StorageController
from qarnotBoxSched import QarnotBoxSched
from qarnotUtils import *

from collections import defaultdict

import math
'''
This is a variant of the qarnotNodeSched where all datasets are replicated on all QBox disks.
'''

class QarnotNodeSchedFullReplicate(QarnotNodeSched):
    def __init__(self, options):
        super().__init__(options)

        if "output_path" in options:
            self.output_filename = options["output_path"] + "/out_pybatsim_full_replicate.csv"
        else:
            self.output_filename = None

        self.qbox_sched_name = QarnotBoxSchedFullReplicate

    def onSimulationBegins(self):
        assert self.bs.dynamic_job_registration_enabled, "Registration of dynamic jobs must be enabled for this scheduler to work"
        assert self.bs.ack_of_dynamic_jobs == False, "Acknowledgment of dynamic jobs must be disabled for this scheduler to work"

        # Register the profile of the cpu-burn jobs. CPU value is 1e20 because it's supposed to be endless and killed when needed.
        self.bs.register_profiles("dyn-burn", {"burn":{"type":"parallel_homogeneous", "cpu":1e20, "com":0, "priority": -23}})
        self.bs.wake_me_up_at(1)

        self.initQBoxesAndStorageController()
        for qb in self.dict_qboxes.values():
            qb.onBeforeEvents()

        self.storage_controller.onSimulationBegins()
        self.storage_controller.replicateAllDatasetsAtStart()

        self.logger.info("[{}]- QNode: End of SimulationBegins".format(self.bs.time()))



class QarnotBoxSchedFullReplicate(QarnotBoxSched):
    def __init__(self, name, dict_qrads, site, bs, qn, storage_controller):
        super().__init__(name, dict_qrads, site, bs, qn, storage_controller)

    def onDispatchedInstance(self, instances, priority_group, qtask_id):
        '''
        Instances is a list of Batsim jobs corresponding to the instances dispatched to this QBox.
        Priority_group of the instances

        Datasets are shared between the instances of the same QTask. So we only need to retrive the datasets once for all instances
        '''
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
            self.dict_subqtasks[qtask_id] = sub_qtask

        # We consider that all datasets are replicated on all QBox disks
        # So don't need to ask for them, they are already here
        sub_qtask.update_waiting_datasets([]) # Just to be sure
        self.scheduleInstances(sub_qtask)
