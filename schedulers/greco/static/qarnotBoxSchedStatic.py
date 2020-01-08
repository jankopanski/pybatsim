from batsim.batsim import Batsim, Job
from qarnotBoxSched import QarnotBoxSched
from qarnotUtils import *

from procset import ProcSet
from collections import defaultdict

import logging


'''
This is a simplification of the qarnotBoxSched scheduler that simply schedules instances on the same
QMobo as for the real execution of the inputs on the Qarnot platform.
'''

class QarnotBoxSchedStatic(QarnotBoxSched):
    def __init__(self, name, dict_qrads, site, bs, qn, storage_controller):
        super().__init__(name, dict_qrads, site, bs, qn, storage_controller)


    def onNoMoreEvents(self):
        self.scheduleInstancesStatic()

        if len(self.jobs_to_kill) > 0:
            self.bs.kill_jobs(self.jobs_to_kill)
            self.jobs_to_kill = []
        if len(self.jobs_to_execute) > 0:
            self.bs.execute_jobs(self.jobs_to_execute)
            self.jobs_to_execute = []

        for pstate, resources in self.stateChanges.items():
            self.bs.set_resource_state(resources, pstate)
        self.stateChanges.clear()


    def onDispatchedInstancesStatic(self, instances, qtask_id):
        if qtask_id in self.dict_subqtasks:
            sub_qtask = self.dict_subqtasks[qtask_id]
            sub_qtask.waiting_instances.extend(instances.copy()) #TODO maybe don't need this copy since we use extend
        else:
            # This is a QTask "unknown" to the QBox.
            # Create and add the SubQTask to the dict
            i = instances[0]
            sub_qtask = SubQTask(qtask_id, PriorityGroup.fromValue(i.profile_dict["priority"]), instances,
                                 self.bs.profiles[i.workload][i.profile]["datasets"])
            self.dict_subqtasks[qtask_id] = sub_qtask

        # Then ask for the data staging of the required datasets
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


    def onDatasetArrivedOnDisk(self, dataset_id):
        '''
        The Storage Controller notifies that the required dataset arrived on the disk.
        Ask for a hard link on this dataset if there are tasks that were waiting for this dataset.
        Then check if we can launch instances.
        '''
        to_launch = []
        if dataset_id in self.waiting_datasets:
            n = self.waiting_datasets.count(dataset_id)
            self.logger.info("[{}]--- Dataset {} arrived on QBox {} and was waited by {} SubQTasks".format(self.bs.time(), dataset_id, self.name, n))
            for sub_qtask in self.dict_subqtasks.values():
                if dataset_id in sub_qtask.waiting_datasets:
                    sub_qtask.waiting_datasets.remove(dataset_id)
                    self.waiting_datasets.remove(dataset_id)
                    self.storage_controller.onQBoxAskHardLink(self.disk_batid, dataset_id, sub_qtask.id)

                    n-= 1
                    if n == 0:
                        # All SubQTasks waiting for this dataset were found, stop
                        assert dataset_id not in self.waiting_datasets # TODO remove this at some point?
                        break


    def scheduleInstancesStatic(self):
        '''
        Try to start instances that have all the datasets and for whose time is >= real_start_time
        to the mobo specified by real_allocation.
        '''

        for sub_qtask in self.dict_subqtasks.values():
            if len(sub_qtask.waiting_datasets) == 0:
                # Start all instances for which time >= real_start_time
                for instance in sub_qtask.waiting_instances.copy():
                    if self.bs.time() >= instance.json_dict["real_start_time"]:
                        # We should start the instance now
                        batid = instance.json_dict["real_allocation"]
                        qm = self.dict_ids[batid].dict_mobos[batid]
                        if (qm.state < QMoboState.fromPriority[sub_qtask.priority_group]) and (qm.state != QMoboState.LAUNCHING):
                            #The mobo is not running an instance of >= priority
                            sub_qtask.waiting_instances.remove(instance)
                            sub_qtask.mark_running_instance(instance)
                            self.startInstance(qm, instance)
                            pstate = computePState(instance.json_dict["real_avg_speed"])
                            self.logger.info("{} started on {} with pstate {} (corresponding to speed {}), real speed {}".format(
                                instance.id, qm.batid, pstate, (100*(40-pstate)), instance.json_dict["real_avg_speed"]))
                            self.stateChanges[pstate].insert(qm.batid)


def computePState(real_speed):
    if real_speed > 4000:
        real_speed = 4000
    elif real_speed < 0:
        real_speed = 0
    return round(40 - (real_speed/100))
