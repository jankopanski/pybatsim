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

class QarnotNodeSchedReplicateOnSubmit(QarnotNodeSched):
    def __init__(self, options):
        super().__init__(options)

        if "output_path" in options:
            self.output_filename = options["output_path"] + "/out_pybatsim_full_replicate.csv"
        else:
            self.output_filename = None

        #self.qbox_sched_name = QarnotBoxSchedFullReplicate


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
            self.nb_received_qtasks += 1
            # Ask for the replication of its datasets
            for dataset_id in qtask.datasets:
                self.storage_controller.replicateDatasetOnAllDisks(dataset_id)
        else:
            qtask = self.qtasks_queue[qtask_id]
        self.nb_received_instances += 1

        qtask.instance_submitted(job, resubmit)

        self.do_dispatch = True
        #TODO maybe we'll need to disable this dispatch, same reason as when a job completes



"""
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
        self.scheduleInstances(sub_qtask)
"""