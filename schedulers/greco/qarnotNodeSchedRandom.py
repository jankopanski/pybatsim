from batsim.batsim import BatsimScheduler, Batsim, Job
from qarnotNodeSched import QarnotNodeSched
from storageController import StorageController
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
import random
'''
This is a variant of the qarnotNodeSched that takes available mobos in a random order to dispatch instances.
'''

class QarnotNodeSchedRandom(QarnotNodeSched):
    def __init__(self, options):
        super().__init__(options)

        if "output_path" in options:
            self.output_filename = options["output_path"] + "/out_pybatsim.csv"
        else:
            self.output_filename = None


    def sortAvailableMobos(self, priority, increasing_slots=True):
        random.shuffle(self.lists_available_mobos)
