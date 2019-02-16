from batsim.batsim import Batsim, Job

from procset import ProcSet
from collections import defaultdict

import logging


'''
This is the qarnot QBox scheduler





#TODO when an event of type "machine_unavailable" is received
# we should mark the qrad/mobos as unavailable as well 


'''

class QarnotBoxSched():
    def __init__(self, name, list_mobos, bs, qn, storage_controller):
        ''' WARNING!!!
        The init of the QBox Schedulers is done upon receiving
        the SimulationBegins in the QNode Scheduler 
        Thus the init of QBox and onSimulationBegins of QBox is quite the same
        '''
        self.bs = bs
        self.qn = qn
        self.storage_controller = storage_controller
        self.logger = bs.logger
        self.name = name

        self.dict_mobos = {} # Maps the batsim ids of the mobos to the properties

        for (index, properties) in list_mobos:
            watts = (properties["watt_per_state"]).split(', ')
            properties["nb_pstates"] = len(watts)
            properties["watt_per_state"] = [float((x.split(':'))[-1]) for x in watts]
            self.dict_mobos[index] = properties
        self.nb_mobos = len(self.dict_mobos)

        self.availBkgd = ProcSet()
        self.availLow = ProcSet()
        self.availHigh = ProcSet()

        # TODO when should be init these? SimuBEGINS or BeforeEvents?
        self.targetTemp = {}
        self.diffTemp = {}

        # Tells the StorageController who we are
        self.storage_controller.onQBoxRegistration(self.name, self)

        self.logger.info("--- QBox {} initialization completed, I have {} mobos under my watch!".format(self.name, self.nb_mobos))

    def onSimulationBegins(self):
        pass

    def onSimulationEnds(self):
        pass

    def onBeforeEvents(self):
        pass

    def onNoMoreEvents(self):
        pass