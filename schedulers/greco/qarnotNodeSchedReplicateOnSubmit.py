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
            self.output_filename = options["output_path"] + "/out_pybatsim.csv"
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
            user_id = job.profile_dict["user"]

            qtask = QTask(qtask_id, job.profile_dict["priority"], list_datasets, user_id)
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
