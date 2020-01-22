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

        self.qbox_sched_name = QarnotBoxSchedFullReplicate


class QarnotBoxSchedFullReplicate(QarnotBoxSched):
    def __init__(self, name, dict_qrads, site, bs, qn, storage_controller):
        super().__init__(name, dict_qrads, site, bs, qn, storage_controller)


    def askForDatasets(self, sub_qtask):
        # We consider that all datasets are replicated on all QBox disks
        # So don't need to ask for them, they are already here
        sub_qtask.update_waiting_datasets([]) # Just to be sure
