from batsim.batsim import BatsimScheduler
from copy import deepcopy

from batsim.batsim import Job

from procset import ProcSet

class TransferHistoryStatic(BatsimScheduler):

    resources_mapping = {}

    def __init__(self, options):
        super().__init__(options)
        
    
    def onSimulationBegins(self):
        for res in self.bs.machines['storage']:
            self.resources_mapping[res["name"]] = res["id"]
            
        print(self.resources_mapping)
        
    def onJobSubmission(self, job):
        print(job.json_dict)
        
        from_str = ""
        to_str = ""
        
        if job.json_dict['direction'] != 'Up':
            from_str = "storage_server"
            to_str = job.json_dict['node']
        else:
            from_str = job.json_dict['node']
            to_str = "storage_server"
        
        source_id = self.resources_mapping[from_str]
        dest_id = self.resources_mapping[to_str]
        
        job_new = Job(job.json_dict['id'], 0, -1, 1, "", "")
        job_new.allocation = ProcSet(source_id, dest_id)
        job_new.storage_mapping = {}
        job_new.storage_mapping[from_str] = source_id
        job_new.storage_mapping[to_str] = dest_id

        self.bs.execute_jobs([job_new])

