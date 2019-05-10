from batsim.batsim import BatsimScheduler

class TransferHistoryStatic(BatsimScheduler):

    def __init__(self, options):
        super().__init__(options)
        
    def onJobSubmission(self, job):
        pass

