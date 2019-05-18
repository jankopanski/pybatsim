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

class QarnotNodeSchedAndrei(QarnotNodeSched):
    def __init__(self, options):
        super().__init__(options)

        if "output_path" in options:
            self.output_filename = options["output_path"] + "/out_pybatsim.csv"
        else:
            self.output_filename = None


    def doDispatch (self):
        ''' First try to dispatch on QBoxes already having the required datasets.
        Then do a regular dispatch.
        '''
        self.doDispatch_by_datasets()
        self.doDispatch_by_priority()


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


    def doDispatch_by_datasets(self):
        '''For the dispatch of a task:
        Select the task of highest priority that have the smallest number of running instances.
        Then to choose the QBoxes/Mobos where to send it:
        - First send as much instances as possible on mobos available for bkgd
            (the ones that are running bkgd tasks, because they have the most "coolness reserve"
            and will likely run faster and longer than idle cpus)

        - Second on mobos available for low
        - Third on mobos available for high

        The QBoxes where the instances are sent are sorted by increasing number of available mobos
        (so keep large blocks of mobos available for cluster tasks)
        Always send as much instances as there are available mobos in a QBox
        '''

        # TODO here we take care only of "regular" tasks with instances and not cluster tasks

        if len(self.qtasks_queue) == 0:
            self.logger.info("[{}]- QNode has nothing to dispatch".format(self.bs.time()))
            return

        # Sort the jobs by decreasing priority (hence the '-' sign) and then by increasing number of running instances
        self.logger.info("[{}]- QNode starting doDispatch".format(self.bs.time()))
        
        for qtask in sorted(self.qtasks_queue.values(),key=lambda qtask:(-qtask.priority, qtask.nb_dispatched_instances)):
            nb_instances_left = len(qtask.waiting_instances)
            if nb_instances_left > 0:
                self.logger.debug("[{}]- QNode trying to dispatch {} of priority {} having {} waiting and {} dispatched instances".format(self.bs.time(),qtask.id, qtask.priority, len(qtask.waiting_instances), qtask.nb_dispatched_instances))
                list_qboxes = self.get_qboxes_with_datasets(qtask.datasets)

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
                        qb.onDispatchedInstance(jobs, PriorityGroup.BKGD, qtask.id) # Dispatch the instances
                        tup[1] -= nb_instances_left                             # Update the number of slots in the list
                        nb_instances_left = 0
                        # No more instances are waiting, stop the dispatch for this qtask
                        break
                    elif nb_slots > 0: # 0 < nb_slots < nb_instances_left
                        # Schedule instances for all slots of this QBox
                        jobs = qtask.waiting_instances[0:nb_slots]
                        self.addJobsToMapping(jobs, qb)
                        qtask.instances_dispatched(jobs)
                        qb.onDispatchedInstance(jobs, PriorityGroup.BKGD, qtask.id)
                        tup[1] = 0
                        nb_instances_left -= nb_slots
                #End for bkgd slots

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
                            qb.onDispatchedInstance(jobs, PriorityGroup.LOW, qtask.id)
                            tup[2] -= nb_instances_left
                            nb_instances_left = 0
                            # No more instances are waiting, stop the dispatch for this qtask
                            break
                        elif nb_slots > 0: # 0 < nb_slots < nb_instances_left
                            # Schedule instances for all slots of this QBox
                            jobs = qtask.waiting_instances[0:nb_slots]
                            self.addJobsToMapping(jobs, qb)
                            qtask.instances_dispatched(jobs)
                            qb.onDispatchedInstance(jobs, PriorityGroup.LOW, qtask.id)
                            tup[2] = 0
                            nb_instances_left -= nb_slots
                    #End for low slots

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
                                qb.onDispatchedInstance(jobs, PriorityGroup.HIGH, qtask.id)
                                tup[3] -= nb_instances_left
                                nb_instances_left = 0
                                # No more instances are waiting, stop the dispatch for this qtask
                                break
                            elif nb_slots > 0: # 0 < nb_slots < nb_instances_left
                                # Schedule instances for all slots of this QBox
                                jobs = qtask.waiting_instances[0:nb_slots]
                                self.addJobsToMapping(jobs, qb)
                                qtask.instances_dispatched(jobs)
                                qb.onDispatchedInstance(jobs, PriorityGroup.HIGH, qtask.id)
                                tup[3] = 0
                                nb_instances_left -= nb_slots
                        #End for high slots
                    #End if high priority and nb_instances_left > 0
                #End if low/high priority and nb_instances_left > 0
                self.logger.debug("[{}]- QNode now dispatched a total of {} instances of {}, {} are still waiting.".format(self.bs.time(),qtask.nb_dispatched_instances, qtask.id, len(qtask.waiting_instances)))
            #End if nb_instances_left > 0
        #End for qtasks in queue
        self.logger.info("[{}]- QNode end of doDispatch".format(self.bs.time()))
    #End of doDispatch function


    def doDispatch_by_priority(self):
        '''For the dispatch of a task:
        Select the task of highest priority that have the smallest number of running instances.
        Then to choose the QBoxes/Mobos where to send it:
        - First send as much instances as possible on mobos available for bkgd
            (the ones that are running bkgd tasks, because they have the most "coolness reserve"
            and will likely run faster and longer than idle cpus)

        - Second on mobos available for low
        - Third on mobos available for high

        The QBoxes where the instances are sent are sorted by increasing number of available mobos
        (so keep large blocks of mobos available for cluster tasks)
        Always send as much instances as there are available mobos in a QBox
        '''

        # TODO here we take care only of "regular" tasks with instances and not cluster tasks

        if len(self.qtasks_queue) == 0:
            self.logger.info("[{}]- QNode has nothing to dispatch".format(self.bs.time()))
            return

        # Sort the jobs by decreasing priority (hence the '-' sign) and then by increasing number of running instances
        self.logger.info("[{}]- QNode starting doDispatch".format(self.bs.time()))
        for qtask in sorted(self.qtasks_queue.values(),key=lambda qtask:(-qtask.priority, qtask.nb_dispatched_instances)):
            nb_instances_left = len(qtask.waiting_instances)
            if nb_instances_left > 0:
                self.logger.debug("[{}]- QNode trying to dispatch {} of priority {} having {} waiting and {} dispatched instances".format(self.bs.time(),qtask.id, qtask.priority, len(qtask.waiting_instances), qtask.nb_dispatched_instances))
                # Dispatch as many instances as possible on mobos available for bkgd, no matter the priority of the qtask
                self.sortAvailableMobos("bkgd")
                for tup in self.lists_available_mobos:
                    qb = self.dict_qboxes[tup[0]]
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
                #End for bkgd slots

                if (nb_instances_left > 0) and (qtask.priority_group > PriorityGroup.BKGD):
                    # There are more instances to dispatch and the qtask is either low or high priority
                    self.sortAvailableMobos("low")
                    for tup in self.lists_available_mobos:
                        qb = self.dict_qboxes[tup[0]]
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
                    #End for low slots

                    if (nb_instances_left > 0) and (qtask.priority_group > PriorityGroup.LOW):
                        # There are more instances to dispatch and the qtask is high priority
                        self.sortAvailableMobos("high")
                        for tup in self.lists_available_mobos:
                            qb = self.dict_qboxes[tup[0]]
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
                        #End for high slots
                    #End if high priority and nb_instances_left > 0
                #End if low/high priority and nb_instances_left > 0
                self.logger.debug("[{}]- QNode now dispatched a total of {} instances of {}, {} are still waiting.".format(self.bs.time(),qtask.nb_dispatched_instances, qtask.id, len(qtask.waiting_instances)))
            #End if nb_instances_left > 0
        #End for qtasks in queue
        self.logger.info("[{}]- QNode end of doDispatch".format(self.bs.time()))
    #End of doDispatch function
