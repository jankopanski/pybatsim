"""
Scheduler used in tests to submit a dynamic workload which originates from the scheduler
(No external workload file exists).
"""

from batsim.sched import Scheduler
from batsim.sched import Profiles
from batsim.sched.workloads import WorkloadDescription

from batsim.sched.algorithms.filling import filler_sched
from batsim.sched.algorithms.utils import default_resources_filter


class DynamicTestScheduler(Scheduler):

    def on_init(self):
        self.submit_dynamic_job(
            walltime=10,
            res=2,
            id=42,
            profile=Profiles.Delay(7))
        self.submit_dynamic_job(walltime=10, res=2, profile=Profiles.Delay(7))
        self.submit_dynamic_job(walltime=10, res=2, profile=Profiles.Delay(7))

        w = WorkloadDescription(name="TestWorkload")
        w.new_job(subtime=0, walltime=10, res=4, profile=Profiles.Delay(5))
        w.new_job(subtime=0, walltime=11, res=4, profile=Profiles.Delay(10))
        w.new_job(walltime=60, res=4, profile=Profiles.Sequence([
            Profiles.Delay(15),
            Profiles.Delay(5),
            Profiles.Delay(10),
            Profiles.Delay(20)]))
        w.new_job(walltime=60, res=4, profile=Profiles.Sequence([
            Profiles.Delay(5),
            Profiles.Sequence([
                Profiles.Delay(15),
                Profiles.Delay(5),
            ])]))

        w.submit(self)

    def schedule(self):
        return filler_sched(self,
                            abort_on_first_nonfitting=True,
                            resources_filter=default_resources_filter)
