from batsim.batsim import BatsimScheduler, Batsim, Job
from qarnotNodeSched import QarnotNodeSched
from qarnotStorageController import QarnotStorageController
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

class QarnotNodeSchedLocalityBased(QarnotNodeSched):
    def __init__(self, options):
        super().__init__(options)



    def doDispatch (self):
        ''' First try to dispatch on QBoxes already having the required datasets.
        Then do a regular dispatch.
        '''
        self.doDispatchByDatasets()
        if len(self.qtasks_queue) > 0:
            self.doDispatchByPriority()


    def get_qboxes_with_datasets(self, datasets):
        ''' Lists all QBoxes that have the required list of datasets from the job 
        Could happen:
        - Required Data Set == NULL => qboxes_list empty
        - Required Data Set != NULL => qboxes_list empty or Not
        '''
        qboxes_list = []
        if len(datasets) > 0:
            qboxes_list = self.storage_controller.onGetStoragesHavingDatasets(datasets)
        return qboxes_list


    def doDispatchByDatasets(self):
        '''
        In this dispatch function, only QBoxes having already the datasets required
        by a QTask are considered for dispatch.
        '''
        if len(self.qtasks_queue) == 0:
            self.logger.info("[{}]- QNode has nothing to dispatch".format(self.bs.time()))
            return

        # Sort the jobs by decreasing priority (hence the '-' sign) and then by increasing number of running instances
        self.logger.info("[{}]- QNode starting doDispatchByPriority".format(self.bs.time()))
        for qtask in sorted(self.qtasks_queue.values(),key=lambda qtask:(-qtask.priority, qtask.nb_dispatched_instances)):
            nb_instances_left = len(qtask.waiting_instances)
            if nb_instances_left > 0:
                self.logger.debug("[{}]- QNode trying to dispatch {} of priority {} having {} waiting and {} dispatched instances".format(self.bs.time(),qtask.id, qtask.priority, len(qtask.waiting_instances), qtask.nb_dispatched_instances))
                list_qboxes = self.get_qboxes_with_datasets(qtask.datasets)

                if qtask.is_cluster():
                    assert len(qtask.waiting_instances) == 1, f"Trying to dispatch a cluster task {qtask.id} having more than one waiting instance, aborting." # Just to be sure

                    # A cluster QTask must be dispatched entirely on the same QBox
                    for tup in self.lists_available_mobos:
                        qb = self.dict_qboxes[tup[0]]
                        if not qb.name in list_qboxes:
                            continue # Don't consider QBoxes that do not have the datasets

                        nb_available_slots = tup[1] # The BKGD ones
                        if qtask.priority_group > PriorityGroup.BKGD:
                            nb_available_slots += tup[2] # The LOW ones
                            if qtask.priority_group > PriorityGroup.LOW:
                                nb_available_slots += tup[3] # The HIGH ones

                        if nb_available_slots >= qtask.requested_resources:
                            jobs = qtask.waiting_instances.copy()
                            qtask.instances_dispatched(jobs)
                            qb = self.dict_qboxes[tup[0]]
                            self.addJobsToMapping(jobs, qb)
                            self.logger.debug("[{}]- QNode now dispatched cluster task {} requiring {} on {} having {} available slots.".format(self.bs.time(),qtask.id, qtask.requested_resources, qb.name, nb_available_slots))
                            qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id, True) # The True argument means that this dispatched instance is a cluster

                            # Remove the used number of slots from the tup, starting from BKGD
                            slots_to_remove = qtask.requested_resources
                            if slots_to_remove <= tup[1]: # Take from BKGD slots
                                tup[1] -= slots_to_remove
                            else:
                                slots_to_remove -= tup[1]
                                tup[1] = 0
                                if slots_to_remove <= tup[2]: # Take from LOW slots
                                    tup[2] -= slots_to_remove
                                else:
                                    slots_to_remove -= tup[2]
                                    tup[2] = 0

                                    assert qtask.priority_group > PriorityGroup.LOW, f"Cluster {qtask.id} of priority <= LOW was dispatched on {qb.name} but there were not enough slots for it..." # This should never happen
                                    assert slots_to_remove <= tup[3], f"Cluster {qtask.id} was dispatched on {qb.name} but there were not enough slots for it..." # This should never happen
                                    tup[3] -= slots_to_remove
                            # End updating slots of tup

                            break # Get out of the for loop
                    #End for tup

                else: # This is a regular QTask with instances
                    # Dispatch as many instances as possible on mobos available for bkgd, no matter the priority of the qtask
                    self.sortAvailableMobos("bkgd")
                    for tup in self.lists_available_mobos:
                        qb = self.dict_qboxes[tup[0]]
                        if not qb.name in list_qboxes:
                            continue # Don't consider QBoxes that do not have the datasets

                        nb_slots = tup[1]
                        if nb_slots >= nb_instances_left:
                            # There are more available slots than instances, gotta dispatch'em all!
                            jobs = qtask.waiting_instances.copy()
                            self.addJobsToMapping(jobs, qb)                     # Add the Jobs to the internal mapping
                            qtask.instances_dispatched(jobs)                    # Update the QTask
                            qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id) # Dispatch the instances
                            tup[1] -= nb_instances_left                             # Update the number of slots in the list
                            nb_instances_left = 0
                            # No more instances are waiting, stop the dispatch for this qtask
                            break
                        elif nb_slots > 0: # 0 < nb_slots < nb_instances_left
                            # Schedule instances for all slots of this QBox
                            jobs = qtask.waiting_instances[0:nb_slots]
                            self.addJobsToMapping(jobs, qb)
                            qtask.instances_dispatched(jobs)
                            qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id)
                            tup[1] = 0
                            nb_instances_left -= nb_slots
                    #End for tup in bkgd slots

                    if (nb_instances_left > 0) and (qtask.priority_group > PriorityGroup.BKGD):
                        # There are more instances to dispatch and the qtask is either low or high priority
                        self.sortAvailableMobos("low")
                        for tup in self.lists_available_mobos:
                            qb = self.dict_qboxes[tup[0]]
                            if not qb.name in list_qboxes:
                                continue # Don't consider QBoxes that do not have the datasets

                            nb_slots = tup[2]
                            if nb_slots >= nb_instances_left:
                                # There are more available slots than instances, gotta dispatch'em all!
                                jobs = qtask.waiting_instances.copy()
                                self.addJobsToMapping(jobs, qb)
                                qtask.instances_dispatched(jobs)
                                qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id)
                                tup[2] -= nb_instances_left
                                nb_instances_left = 0
                                # No more instances are waiting, stop the dispatch for this qtask
                                break
                            elif nb_slots > 0: # 0 < nb_slots < nb_instances_left
                                # Schedule instances for all slots of this QBox
                                jobs = qtask.waiting_instances[0:nb_slots]
                                self.addJobsToMapping(jobs, qb)
                                qtask.instances_dispatched(jobs)
                                qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id)
                                tup[2] = 0
                                nb_instances_left -= nb_slots
                        #End for tup low slots

                        if (nb_instances_left > 0) and (qtask.priority_group > PriorityGroup.LOW):
                            # There are more instances to dispatch and the qtask is high priority
                            self.sortAvailableMobos("high")
                            for tup in self.lists_available_mobos:
                                qb = self.dict_qboxes[tup[0]]
                                if not qb.name in list_qboxes:
                                    continue # Don't consider QBoxes that do not have the datasets

                                nb_slots = tup[3]
                                if nb_slots >= nb_instances_left:
                                    # There are more available slots than wild instances, gotta catch'em all!
                                    jobs = qtask.waiting_instances.copy()
                                    self.addJobsToMapping(jobs, qb)
                                    qtask.instances_dispatched(jobs)
                                    qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id)
                                    tup[3] -= nb_instances_left
                                    nb_instances_left = 0
                                    # No more instances are waiting, stop the dispatch for this qtask
                                    break
                                elif nb_slots > 0: # 0 < nb_slots < nb_instances_left
                                    # Schedule instances for all slots of this QBox
                                    jobs = qtask.waiting_instances[0:nb_slots]
                                    self.addJobsToMapping(jobs, qb)
                                    qtask.instances_dispatched(jobs)
                                    qb.onDispatchedInstance(jobs, qtask.priority_group, qtask.id)
                                    tup[3] = 0
                                    nb_instances_left -= nb_slots
                            #End for tup high slots
                        #End if high priority and nb_instances_left > 0
                    #End if low/high priority and nb_instances_left > 0
                    self.logger.debug("[{}]- QNode now dispatched a total of {} instances of {}, {} are still waiting.".format(self.bs.time(),qtask.nb_dispatched_instances, qtask.id, len(qtask.waiting_instances)))
                #End if qtask.is_cluster()
            #End if nb_instances_left > 0
        #End for qtasks in queue
        self.logger.info("[{}]- QNode end of doDispatchByPriority".format(self.bs.time()))
    #End of doDispatchByPriority function
