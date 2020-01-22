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

class QarnotNodeSchedReplicate10LeastLoaded(QarnotNodeSched):
    def __init__(self, options):
        super().__init__(options)

        self.nb_least_loaded = 10


    def doDatasetReplication(self, qtask):
        # Replicate on 3 least loaded disks
        storage_ids = self.storage_controller.getNMostEmptyStorages(self.nb_least_loaded)
        for dataset_id in qtask.datasets:
            self.storage_controller.replicateDatasetOnStorages(dataset_id, storage_ids)
