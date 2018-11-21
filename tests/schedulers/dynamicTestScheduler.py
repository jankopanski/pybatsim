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
        self.register_dynamic_job(
            walltime=10,
            res=2,
            id=42,
            profile=Profiles.Delay(7))
        self.register_dynamic_job(walltime=10, res=2, profile=Profiles.Delay(5))
        self.register_dynamic_job(walltime=10, res=2, profile=Profiles.Delay(6))

        w = WorkloadDescription(name="TestWorkload")
        w.new_job(subtime=0, walltime=10, res=4, profile=Profiles.Delay(5))
        w.new_job(subtime=0, walltime=11, res=4, profile=Profiles.Delay(10))
        w.new_job(walltime=60, res=4, profile=Profiles.Sequence([
            Profiles.Delay(15),
            Profiles.Delay(4),
            Profiles.Delay(11),
            Profiles.Delay(20)]))
        w.new_job(walltime=60, res=4, profile=Profiles.Sequence([
            Profiles.Delay(3),
            Profiles.Delay(14),
            Profiles.Delay(7),
            ]))

        w.submit(self)
        self.notify_registration_finished()

    def schedule(self):
        return filler_sched(self,
                            abort_on_first_nonfitting=True,
                            resources_filter=default_resources_filter)
