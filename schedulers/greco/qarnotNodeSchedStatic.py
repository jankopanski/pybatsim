from batsim.batsim import BatsimScheduler, Batsim, Job
from qarnotNodeSched import QarnotNodeSched
from StorageController import StorageController
from qarnotBoxSchedStatic import QarnotBoxSchedStatic
from qarnotUtils import *

from collections import defaultdict

import math

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

        to_reject = []
        dict_dispatch = defaultdict(lambda: defaultdict(list)) # Maps the QBoxSched to a dict
                                                       # that maps the QTask id to a list of instances to be dispatched there

        for qtask in self.qtasks_queue.values():
            for instance in qtask.waiting_instances.copy():
                # Dispatch the instance directly when it is submitted
                real_alloc = instance.json_dict["real_allocation"]
                if real_alloc == -1:
                    # The real allocation was not in the platform
                    qtask.instances_dispatched([instance])
                    to_reject.append(instance)
                else:
                    qb = self.dict_resources[real_alloc]
                    dict_dispatch[qb][qtask.id].append(instance)

        for qb, sub_dict in dict_dispatch.items():
            for qid, instances in sub_dict.items():
                self.addJobsToMapping(instances, qb)
                self.qtasks_queue[qid].instances_dispatched(instances)
                qb.onDispatchedInstancesStatic(instances, qid)

        if len(to_reject) > 0:
            self.logger.info("[{}]- QNodeStatic has rejected {} instances because the QMobos are not in the platform".format(self.bs.time(), len(to_reject)))
            self.bs.reject_jobs(to_reject)

    '''Just some guards to be sure '''
    def doDispatch(self):
        assert False, "Regular dispatch requested, this is not possible."

    def onQBoxRejectedInstances(self, instances, qb_name):
        assert False, "{} rejected {} instances, this is not possible.".format(qb_name, len(instances))

    def tryDirectDispatch(self, qtask, qb):
        assert False, "Direct dispatch requested, this is not possible."