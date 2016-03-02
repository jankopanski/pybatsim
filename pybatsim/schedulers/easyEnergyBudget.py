#/usr/bin/python3

from easyBackfill import *


class EasyEnergyBudget(EasyBackfill):
    def __init__(self, options):
        self.options = options
        
        self.budget_total = options["budget_total"]
        self.budget_start = options["budget_start"]#the budget limitation start at this time included
        self.budget_end = options["budget_end"]#the budget end just before this time
        
        self.allow_FCFS_jobs_to_use_budget_saved_measured = options["allow_FCFS_jobs_to_use_budget_saved_measured"]
        
        self.reduce_powercap_to_save_energy = options["reduce_powercap_to_save_energy"]
        self.estimate_energy_jobs_to_save_energy = not self.reduce_powercap_to_save_energy
        
        
        self.budget_saved_measured = 0.0
        self.budget_reserved = 0.0
        self.listJobReservedEnergy = []
        
        self.power_idle = options["power_idle"]
        self.power_compute = options["power_compute"]
        
        self.powercap = self.budget_total/(self.budget_end-self.budget_start)
        
        self.already_asked_to_awaken_at_end_of_budget = False
        
        self.monitoring_regitered = False
        self.monitoring_period = 5
        self.monitoring_last_value = None
        self.monitoring_last_value_time = None

        if self.reduce_powercap_to_save_energy == self.estimate_energy_jobs_to_save_energy:
            assert False, "can't activate reduce_powercap_to_save_energy and estimate_energy_jobs_to_save_energy"


        super(EasyEnergyBudget, self).__init__(options)



    def onJobCompletion(self, job):
        try:
            self.listJobReservedEnergy.remove(job)
            if not hasattr(job, "last_power_monitoring"):
                job.last_power_monitoring = job.start_time
            self.budget_reserved -= job.reserved_power*(job.requested_time -(job.last_power_monitoring - job.start_time))
            assert self.budget_reserved >= 0
        except ValueError:
            pass
        
        super(EasyEnergyBudget, self).onJobCompletion(job)



    def regiter_next_monitoring_event(self):
        next_event = self.bs.time()+self.monitoring_period
        if self.bs.time() >= self.budget_end:
            return
        if next_event > self.budget_end:
            next_event = self.budget_end
        self.bs.wake_me_up_at(next_event)



    def onJobSubmission(self, just_submitted_job):
        if not self.monitoring_regitered:
            self.monitoring_regitered = True
            self.bs.wake_me_up_at(self.budget_start)

        super(EasyEnergyBudget, self).onJobSubmission(just_submitted_job)



    def onReportEnergyConsumed(self, consumed_energy):
        
        if self.monitoring_last_value is None:
            self.monitoring_last_value = consumed_energy
            self.monitoring_last_value_time = self.bs.time()
        
        consumed_energy = consumed_energy - self.monitoring_last_value
        
        maximum_energy_consamble = (self.bs.time()-self.monitoring_last_value_time)*self.powercap
        
        self.budget_saved_measured += maximum_energy_consamble - consumed_energy
        
        
        self.monitoring_last_value = consumed_energy
        self.monitoring_last_value_time = self.bs.time()
        
        for j in self.listJobReservedEnergy:
            assert j in self.listRunningJob
            if not hasattr(job, "last_power_monitoring"):
                job.last_power_monitoring = job.start_time
            self.budget_reserved -= job.reserved_power*(self.bs.time()-job.last_power_monitoring)
            assert self.budget_reserved >= 0
            job.last_power_monitoring = self.bs.time()
        
        self.regiter_next_monitoring_event()



    def onNOP(self):
        current_time = self.bs.time()
        
        if current_time >= self.budget_start:
            self.bs.request_consumed_energy()
        
        if current_time >= self.budget_end:
            self._schedule_jobs(current_time)



    def estimate_energy_running_jobs(self, current_time):
        e = 0
        for j in self.listRunningJob:
            rt = min(j.estimate_finish_time, self.budget_end)
            e += j.requested_resources*self.power_compute*(rt-current_time)
        return e

    def estimate_if_job_fit_in_energy(self, job, start_time, listFreeSpace=None, canUseBudgetLeft=False):
        """
        compute the power consumption of the cluster
        if <job> is not Non,e then add the power consumption of this job as if it were running on the cluster.
        """
        
        #if the job does not cross the budget interval
        if not(start_time < self.budget_end and self.budget_start <= start_time+job.requested_time):
            return True
        
        if listFreeSpace is None:
            listFreeSpace = self.listFreeSpace
        
        free_procs = listFreeSpace.free_processors - job.requested_resources
        
        used_procs = self.bs.nb_res - free_procs
        
        power = used_procs*self.power_compute + free_procs*self.power_idle
        
        pc = (power <= self.powercap)
        if pc:
            return True
        if not canUseBudgetLeft:
            return pc

        energy_excess = (power-self.powercap)*job.requested_time
        
        if energy_excess <= self.budget_saved_measured-self.budget_reserved:
            self.budget_reserved += energy_excess
            job.reserved_power = (power-self.powercap)
            self.listJobReservedEnergy.append(job)
            return True
        return False



    def allocJobFCFS(self, job, current_time):
        #overrinding parent method
        
        if not self.estimate_if_job_fit_in_energy(job, current_time, canUseBudgetLeft=self.allow_FCFS_jobs_to_use_budget_saved_measured):
            return None
        
        return super(EasyEnergyBudget, self).allocJobFCFS(job, current_time)



    def allocJobBackfill(self, job, current_time):
        #overrinding parent method
            
        
        if not self.estimate_if_job_fit_in_energy(job, current_time):
            return None
        
        return super(EasyEnergyBudget, self).allocJobBackfill(job, current_time)



    def findAllocFuture(self, job):
        #overrinding parent method

        listFreeSpaceTemp = copy.deepcopy(self.listFreeSpace)
        
        if len(self.listRunningJob) == 0:
            #this condition happen if the powercap is so low that no job can be started now (perhaps some jobs will be backfilled, but we do not it yet)
            #we will ask batsim to wake up us at the end of the budget interval, just to be sure.
            if (self.bs.time() < self.budget_end) and not self.already_asked_to_awaken_at_end_of_budget:
                self.already_asked_to_awaken_at_end_of_budget = True
                self.bs.wake_me_up_at(self.budget_end)
            
            alloc = listFreeSpaceTemp.assignJob(listFreeSpaceTemp.firstItem, job, self.budget_end)
            #we find a valid allocation
            return (alloc, self.budget_end)

        previous_current_time = self.bs.time()
        for j in self.listRunningJob:
            new_free_space_created_by_this_unallocation = listFreeSpaceTemp.unassignJob(j)

            fit_in_procs = job.requested_resources <= new_free_space_created_by_this_unallocation.res
            
            fit_in_energy = self.estimate_if_job_fit_in_energy(job, j.estimate_finish_time, listFreeSpaceTemp, canUseBudgetLeft=self.allow_FCFS_jobs_to_use_budget_saved_measured)

            if fit_in_procs and fit_in_energy:
                alloc = listFreeSpaceTemp.assignJob(new_free_space_created_by_this_unallocation, job, j.estimate_finish_time)
                #we find a valid allocation
                return (alloc, j.estimate_finish_time)
            
            previous_current_time = j.estimate_finish_time
        
        #We can be here only if a the budget end later that the last running job and that the current job does not fit in energy
        alloc = listFreeSpaceTemp.assignJob(new_free_space_created_by_this_unallocation, job, self.budget_end)
        #we find a valid allocation
        return (alloc, self.budget_end)
    


    def allocJobBackfillWithEnergyCap(self, job, current_time):
        if job.requested_resources*job.requested_time*self.power_compute > self.energycap:
            return None
            
        for l in self.listFreeSpace.generator():
            if job.requested_resources <= l.res and job.requested_time <= l.length:
                alloc = self.listFreeSpace.assignJob(l, job, current_time)
                self.energycap -= job.requested_resources*job.requested_time*self.power_compute
                return alloc
        return None
        


    def findBackfilledAllocs(self, current_time, first_job_starttime):
        
        #if not within the energy budget
        if not(self.allow_FCFS_jobs_to_use_budget_saved_measured) or not(current_time < self.budget_end and self.budget_start <= current_time):
            return super(EasyEnergyBudget, self).findBackfilledAllocs(current_time, first_job_starttime)
        
        elif self.reduce_powercap_to_save_energy:
            pc = self.powercap
            self.powercap -= self.budget_reserved/(first_job_starttime-current_time)
            
            ret = super(EasyEnergyBudget, self).findBackfilledAllocs(current_time, first_job_starttime)
            
            self.powercap = pc
            return ret
        elif self.estimate_energy_jobs_to_save_energy:
            self.allocJobBackfill_backup = self.allocJobBackfill
            self.allocJobBackfill = self.allocJobBackfillWithEnergyCap
            self.energycap = self.powercap*(first_job_starttime-current_time)- self.estimate_energy_running_jobs(current_time)-self.budget_reserved
            self.energycap_endtime = first_job_starttime
            
            ret = super(EasyEnergyBudget, self).findBackfilledAllocs(current_time, first_job_starttime)
            
            self.allocJobBackfill = self.allocJobBackfill_backup
            return ret
        else:
            assert False, "can't activate reduce_powercap_to_save_energy and estimate_energy_jobs_to_save_energy"
    
    def allocBackFill(self, first_job, current_time):
        
        ret = super(EasyEnergyBudget, self).allocBackFill(first_job, current_time)
        
        if first_job in self.listJobReservedEnergy:
            #remove the energy reservation
            self.budget_reserved += first_job.reserved_power*first_job.requested_time
        
        
        return ret
        