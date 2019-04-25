from batsim.batsim import BatsimScheduler, Batsim, Job
from qarnotNodeSched import QarnotNodeSched
from StorageController import StorageController
from qarnotBoxSchedStatic import QarnotBoxSchedStatic
from qarnotUtils import *

from procset import ProcSet
from collections import defaultdict
from copy import deepcopy

import math
import logging
import sys
import os

import csv
import json
'''
This is a simplification of the qarnotNodeSched scheduler that simply schedules instances on the same
QMobo as for the real execution of the inputs on the Qarnot platform.
'''

class QarnotNodeSchedStatic(QarnotNodeSched):
    def __init__(self, options):
        super().__init__(options)

        if "output_path" in options:
            self.output_filename = options["output_path"] + "/out_pybatsim_static.csv"
        else:
            self.output_filename = None

        self.direct_dispatch_enabled = False

        self.qbox_sched_name = QarnotBoxSchedStatic

        self.do_dispatch = False


    def onBeforeEvents(self):
        if self.bs.time() >= self.next_print:
            print(self.bs.time(), (math.floor(self.bs.time())/86400.0))
            self.next_print+=43200
        if self.bs.time() > self.max_simulation_time:
            self.logger.info("[{}] SYS EXIT".format(self.bs.time()))
            sys.exit(1)

        self.logger.info("\n")
        for qb in self.dict_qboxes.values():
            qb.onBeforeEvents()

        if self.bs.time() >= self.time_next_update:
            #self.updateAllQBoxes()

            self.time_next_update = math.floor(self.bs.time()) + self.update_period

            if self.very_first_update: # First update at t=1, next updates every 30 seconds starting at t=30
                self.time_next_update -= 1
                self.very_first_update = False
                self.update_in_current_step = True # We will ask for the next wake up in onNoMoreEvents if the simulation is not finished yet


    def onNoMoreEvents(self):
        if self.end_of_simulation_asked:
            return

        if (self.bs.time() >= 1.0) :#and self.do_dispatch:
            self.doStaticDispatch()

        for qb in self.dict_qboxes.values():
            qb.onNoMoreEvents()

        # If the simulation is not finished and we need to ask Batsim for the next waking up
        if not self.end_of_simulation_asked and self.update_in_current_step:
            self.bs.wake_me_up_at(self.time_next_update)
            self.update_in_current_step = False


    def doStaticDispatch(self):
        ''' Dispatch each instance on the QBox/QMobo that corresponds to the job.real_allocation '''
        if len(self.qtasks_queue) == 0:
            self.logger.info("[{}]- QNode has nothing to dispatch".format(self.bs.time()))
            return

        for qtask in self.qtasks_queue.values():
            for instance in qtask.waiting_instances.copy():
                    # Dispatch the instance directly when it is submitted
                    qb = self.dict_resources[instance.json_dict["real_allocation"]]
                    self.addJobsToMapping([instance], qb)
                    qtask.instances_dispatched([instance])
                    qb.onDispatchedInstanceStatic(instance, qtask.id)

    '''Just some guards to be sure '''
    def doDispatch(self):
        assert False, "Regular dispatch requested, this is not possible."

    def onQBoxRejectedInstances(self, instances, qb_name):
        assert False, "{} rejected {} instances, this is not possible.".format(qb_name, len(instances))

    def tryDirectDispatch(self, qtask, qb):
        assert False, "Direct dispatch requested, this is not possible."