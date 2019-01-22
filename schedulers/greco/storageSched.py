from batsim.batsim import BatsimScheduler, Batsim, Job

import sys
import os
import logging
from procset import ProcSet


class StorageSched(BatsimScheduler):

    def myprint(self,msg):
        print("[{time}] {msg}".format(time=self.bs.time(), msg=msg))


    def __init__(self, options):
        super().__init__(options)

    def onSimulationBegins(self):
        self.bs.logger.setLevel(logging.ERROR)

        for machine in self.bs.machines["storage"]:
            print(machine["id"], machine["name"])

        self.m1 = self.bs.machines["storage"][0]["id"]  # id of first storage machine
        self.m2 = self.bs.machines["storage"][-1]["id"] # id of the last, which is the storage_server

        self.idSub = 0 # Used to generate the next id of the communication job dynamically submitted


        #self.option1()
        self.bs.wake_me_up_at(1000)


    def onJobSubmission(self, job):
        if (job.id).split("!")[0] != "dyn": # If the job comes from the workload
            self.bs.reject_jobs([job])
        else:
            job.allocation = ProcSet(self.m1,self.m2)
            self.bs.execute_jobs([job])

    def onJobCompletion(self, job):
        self.myprint("Job_finished: " + job.id)

    def onNoMoreEvents(self):
        pass

    def onRequestedCall(self):
        self.option2()
        self.bs.notify_registration_finished()


    def option1(self):
        '''
        The list of resources given for the allocation of a job is in increasing order of ids
        so we have to be careful when describing a communication betweem src and dest.
        If the src_id < dest_id then the matrix of comm will look like the one in profile1.
        If src_id > dest_id (here a comm from the storage_server to a qbox_disk for example)
        then the matrix of comm is inversed and looks like the one in profile2.
        '''
        profiles = {"commUP": {'type': 'parallel','cpu': [0,0],'com': [0, 1e8, 
                                                                      0, 0]},
                    "commDOWN": {'type': 'parallel','cpu': [0,0],'com': [0, 0, 
                                                                      1e8, 0]}}

        self.bs.register_profiles("dyn", profiles)
        
        toSched = []

        jid1 = "dyn!" + str(self.idSub)
        self.idSub += 1
        self.bs.register_job(id=jid1, res=2, walltime=-1, profile_name="commUP")
        job1 = Job(jid1, 0, -1, 1, "", "")
        job1.allocation = ProcSet(self.m1, self.m2)
        toSched.append(job1)

        jid2 = "dyn!" + str(self.idSub)
        self.idSub += 1
        self.bs.register_job(id=jid2, res=2, walltime=-1, profile_name="commDOWN")
        job2 = Job(jid2, 0, -1, 1, "", "")
        job2.allocation = ProcSet(self.m1, self.m2)
        toSched.append(job2)

        self.bs.execute_jobs(toSched)


    def option2(self):
        profiles = {
            "commUP2" : {'type' : 'data_staging', 'nb_bytes' : 1e8, 'from' : 'qb_disk', 'to' : 'ceph'},
            "commDOWN2" : {'type' : 'data_staging', 'nb_bytes' : 1e8, 'from' : 'ceph', 'to' : 'qb_disk'}
        }
        self.bs.register_profiles("dyn", profiles)

        toSched = []

        jid1 = "dyn!" + str(self.idSub)
        self.idSub += 1
        self.bs.register_job(id=jid1, res=1, walltime=-1, profile_name="commUP2")
        job1 = Job(jid1, 0, -1, 1, "", "")
        job1.allocation = ProcSet(self.m1, self.m2)
        job1.storage_mapping = {'qb_disk':self.m1, 'ceph':self.m2}
        toSched.append(job1)

        jid2 = "dyn!" + str(self.idSub)
        self.idSub += 1
        self.bs.register_job(id=jid2, res=1, walltime=-1, profile_name="commDOWN2")
        job2 = Job(jid2, 0, -1, 1, "", "")
        job2.allocation = ProcSet(self.m1, self.m2)
        job2.storage_mapping = {'qb_disk':self.m1, 'ceph':self.m2}
        toSched.append(job2)

        self.bs.execute_jobs(toSched)
