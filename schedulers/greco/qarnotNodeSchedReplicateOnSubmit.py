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

    def doDatasetReplication(self, qtask):
        # Ask for the replication on all QBox disks
        for dataset_id in qtask.datasets:
            self.storage_controller.replicateDatasetOnAllDisks(dataset_id)
