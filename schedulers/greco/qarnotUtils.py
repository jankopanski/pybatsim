from procset import ProcSet

class PriorityGroup:
    BKGD, LOW, HIGH = range(3)
    '''
    If priority in (-max_int, -10) then BKGD
                   [-10,0) then LOW
                   [0,+10] then HIGH
    '''

class QTask:
    # Used by the QNode scheduler
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
        return (len(self.waiting_instances) == 0) and (self.nb_received_instances == self.nb_terminated_instances)

    def instance_rejected(self, job):
        # The instance was rejected by the QBox it was dispatched to
        self.waiting_instances.append(job)
        self.nb_dispatched_instances -= 1

    def instance_submitted(self, job):
        # An instance was submitted by Batsim
        job.priority_group = self.priority_group
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


class SubQTask:
    # Used by the QBox scheduler
    def __init__(self, id, priority_group, instances):
        self.id = id
        self.priority_group = priority_group
        self.waiting_instances = instances      # List of batsim jobs that are waiting to be started
        self.running_instances = []             # List of batsim jobs that are currently running
        self.waiting_datasets = []   # Datasets that are waiting to be on disk

        d = instances[0].profile_dict["datasets"] # List of input datasets
        self.datasets = d if d is not None else []
        '''if d is not None:
            self.datasets = d
        else:
            self.datasets = []'''



class QMoboState:
    OFF, IDLE, RUNBKGD, RUNLOW, RUNHIGH = range(5)
    # When in OFF, the batsim host should be in the last pstate or marked as unavailable
    fromPriority = {
        PriorityGroup.BKGD : RUNBKGD,
        PriorityGroup.LOW : RUNLOW,
        PriorityGroup.HIGH : RUNHIGH
    }

class QRad:
    def __init__(self, name, bs):
        self.name = name
        self.bs = bs
        self.dict_mobos = {} # Maps the batid of the mobo to the QMobo object
        self.targetTemp = 20 # Temperature required in the room
        self.diffTemp = 0    # targetTemp - airTemp: If positive, we need to heat! (This can be viewed as heating capacity)
        self.properties = {} # The simgrid properties of the first mobo (should be the same for all mobos)
        self.pset_mobos = ProcSet()


class QMobo:
    def __init__(self, name, batid, max_pstate):
        self.name = name             # The name of the mobo
        self.batid = batid           # The Batsim id of the mobo
        self.pstate = 0              # The power state of the mobo
        self.max_pstate = max_pstate # The last power state (corresponds to the state OFF)
        self.state = QMoboState.IDLE  # The state of the mobo
        self.running_job = -1        # The Job running on this mobo


    def push_job(self, job):
        assert self.running_job == -1, "Job {} placed on mobo {} that was already executing job {}".format(self.running_job.id, self.name, job.id)
        self.running_job = job
        self.state = QMoboState.fromPriority[job.priority_group]

    def pop_job(self):
        assert self.running_job != -1, "Kill required on mobo {} but it is not running any job".format(self.name)
        job = self.running_job
        self.running_job = -1
        self.state = QMoboState.IDLE

        return job

    def push_direct_job(self, job):
        assert self.running_job.qtask.id == job.qtask.id, "Direct restart of instance {} on mobo {} but previous instance is of different QTask ({})".format(job.id, self.name, self.running_job.id)
        self.running_job = job


