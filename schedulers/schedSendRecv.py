"""
    schedSendRecv
    ~~~~~~~~~~~~~

    Simple scheduler to show the send and receive messages. Should be used with
    a workload consisting of send and receive profiles.

"""

from batsim.sched import Scheduler
from batsim.sched.algorithms.filling import filler_sched
from batsim.sched.algorithms.utils import consecutive_resources_filter


class SchedSendRecv(Scheduler):

    def on_job_message(self, job, message):
        if message.type == "accept":
            job.send("accepted")
        else:
            job.send("denied")

    def schedule(self):
        return filler_sched(
            self,
            resources_filter=consecutive_resources_filter,
            abort_on_first_nonfitting=False)
