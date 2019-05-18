from batsim.batsim import BatsimScheduler, Batsim, Job
from qarnotNodeSched import QarnotNodeSched
from StorageController import StorageController
from qarnotBoxSched import QarnotBoxSched
from qarnotUtils import *

from procset import ProcSet
from collections import defaultdict
from copy import deepcopy

import math
import logging
import sys
import os
import csv
'''
This is a variant of the qarnotNodeSched that takes into account locality of the datasets
to dispatch instances.
'''

class QarnotNodeSchedReplicateLeastLoaded(QarnotNodeSched):
    def __init__(self, options):
        super().__init__(options)

        if "output_path" in options:
            self.output_filename = options["output_path"] + "/out_pybatsim.csv"
        else:
            self.output_filename = None

        self.nb_least_loaded = 5



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
            storage_ids = self.storage_controller.getNMostEmptyStorages(self.nb_least_loaded)
            for dataset_id in qtask.datasets:
                self.storage_controller.replicateDatasetOnStorages(dataset_id, storage_ids)
        else:
            qtask = self.qtasks_queue[qtask_id]
        self.nb_received_instances += 1

        qtask.instance_submitted(job, resubmit)

        self.do_dispatch = True
        #TODO maybe we'll need to disable this dispatch, same reason as when a job completes
