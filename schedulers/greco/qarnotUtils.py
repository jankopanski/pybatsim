from procset import ProcSet

class PriorityGroup:
    BKGD, LOW, HIGH = range(3)
    '''
    If priority in (-max_int, -10) then BKGD
                   [-10,0) then LOW
                   [0,+10] then HIGH
    '''

class QTask:
    def __init__(self, id, priority):
        self.id = id
        self.waiting_instances = [] # List of Batsim Jobs waiting to be scheduled
        self.nb_received_instances = 0    # Number of jobs submitted by Batsim
        self.nb_dispatched_instances = 0  # Number of jobs disatched to the QBoxes
        self.nb_terminated_instances = 0  # Number of jobs that have finished correctly (not killed)

        self.priority = int(priority)
        self.priority_group = PriorityGroup.HIGH if self.priority >= 0 else ( PriorityGroup.BKGD if self.priority < -10 else PriorityGroup.LOW )


        #TODO
        ''' At some point we'll need to re-submit dynamic jobs that have been killed
        due to a rad too hot or a higher priority jobs scheduled on the mobo.
        '''


    def is_complete(self):
        #TODO make sure all instances of a task arrives at the same time in the workload
        return (len(self.waiting_instances) == 0) and (self.received_instances == self.terminated_instances)

    def instance_rejected(self, job):
        # The instance was rejected by the QBox it was dispatched to
        self.waiting_instances.append(job)
        self.nb_dispatched_instances -= 1

    def instance_submitted(self, job):
        # An instance was submitted by Batsim
        self.waiting_instances.append(job)
        self.nb_received_instances += 1

    def instances_dispatched(self, jobs):
        # These instances were dispatched to a QBox
        for job in jobs:
            self.waiting_instances.remove(job)
        self.nb_dispatched_instances += len(jobs)

    def instance_poped_and_dispatched(self):
        # A quick dispatch of an instance of this QTask is required
        self.nb_dispatched_instances += 1
        return self.waiting_instances.pop()

    def instance_finished(self):
        # An instance finished successfully
        self.nb_dispatched_instances -= 1
        self.nb_terminated_instances += 1

    def instance_killed(self):
        # An instance was killed by the QBox scheduler
        self.nb_dispatched_instances -= 1



class FrequencyRegulator:
    def __init__(self, qbox, qrad_properties, bs):
        self.bs = bs                                # PyBatsim
        self.qbox = qbox                            # The QBoxSched
        self.qrad_properties = qrad_properties      # The dict of qrad properties, keys are batsim resource ids
                                                    # and values are dicts of properties ("watt_per_state", "nb_pstates", temperature-related properties)

    #TODO TODO TODO

