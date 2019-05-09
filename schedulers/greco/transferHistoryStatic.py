from batsim.batsim import BatsimScheduler


class TransferHistoryStatic(BatsimScheduler):

    def onAfterBatsimInit(self):
        # You now have access to self.bs and all other functions
        pass

    def onSimulationBegins(self):
        pass

    def onSimulationEnds(self):
        pass

    def onDeadlock(self):
        raise ValueError(
            "[PYBATSIM]: Batsim is not responding (maybe deadlocked)")

    def onJobSubmission(self, job):
        raise NotImplementedError()

    def onJobCompletion(self, job):
        raise NotImplementedError()

    def onJobMessage(self, timestamp, job, message):
        raise NotImplementedError()

    def onJobsKilled(self, jobs):
        raise NotImplementedError()

    def onMachinePStateChanged(self, nodeid, pstate):
        raise NotImplementedError()

    def onReportEnergyConsumed(self, consumed_energy):
        raise NotImplementedError()

    def onAddResources(self, to_add):
        raise NotImplementedError()

    def onRemoveResources(self, to_remove):
        raise NotImplementedError()

    def onRequestedCall(self):
        raise NotImplementedError()

    def onNoMoreJobsInWorkloads(self):
        self.logger.info("There is no more static jobs in the workoad")

    def onNoMoreExternalEvents(self):
        self.logger.info("There is no more external events to occur")

    def onNotifyEventMachineUnavailable(self, machines):
        raise NotImplementedError()

    def onNotifyEventMachineAvailable(self, machines):
        raise NotImplementedError()

    def onBeforeEvents(self):
        pass

    def onNoMoreEvents(self):
        pass

