
class BatsimReply:

    pass


class ConsumedEnergyReply(BatsimReply):

    def __init__(self, consumed_energy):
        self.consumed_energy = consumed_energy

