
from batsim.batsim import BatsimScheduler, Batsim, Job
from batsim.tools.launcher import launch_scheduler, instanciate_scheduler
from StorageController import *
from collections import defaultdict

import sys
import os
import logging

'''
Upon arrival of task j:
	L <- List of QBoxes already having the required data sets ---> How to aks to the storage?
	if L is not empty:
		Dispatch as many instances of j as possible on the QBoxes in L,  ---> Should I write on the matrix like qbox[id] = job ?
		giving priority to QBoxes that have available QRads without preemption,  
		then priority to QRads among QBoxes that requires the most work in the next hour.
	else
 		For all QBoxes, ask the storage controller a download time prediction of the data sets. ---> How?
		L_QBoxes <- List of QBoxes sorted as a function of:     ---> Does this sort function exist?
			1 the count of available QRads for the priority of j in descending order;      
			2 the predicted download time of the data sets.     
		Dispatch as many instances of j as possible on the first m QBoxes of L_QBoxes
'''

class QNodeSched(BatsimScheduler):
    def __init__(self, options):
        super().__init__(options)
        
        if not "qbox_sched" in options:
            print("No qbox_sched provided in json options, using qBoxSched by default.")
            self.qbox_sched_name = "schedulers/greco/qBoxSched.py"
        else:
            self.qbox_sched_name = options["qbox_sched"]

        self.dict_qboxes = {}    # Maps the QBox id to the QBoxSched object
        self.dict_resources = {} # Maps the batsim resource id to the QBoxSched object
        self.heat_requirements = {}
        self.jobs_mapping = {}        # Maps the job id to the qbox id where it has been sent to
        self.waiting_jobs = []