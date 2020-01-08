from procset import ProcSet

class PriorityGroup:
    BKGD, LOW, HIGH = range(3)

    def fromValue(priority):
        return PriorityGroup.HIGH if priority >= 0 else ( PriorityGroup.BKGD if priority < -10 else PriorityGroup.LOW )
        '''
        If priority in (-max_int, -10) then BKGD
                       [-10,0) then LOW
                       [0,+10] then HIGH
        '''

class QTask:
    # Used by the QNode scheduler
    def __init__(self, id, priority, list_datasets, user_id):
        self.id = id
        self.waiting_instances = [] # List of Batsim Jobs waiting to be scheduled
        self.nb_received_instances = 0    # Number of jobs submitted by Batsim
        self.nb_dispatched_instances = 0  # Number of jobs disatched to the QBoxes
        self.nb_terminated_instances = 0  # Number of jobs that have finished correctly (not killed)
        self.nb_killed_instances = 0

        self.is_cluster = False # Whether it is a cluster or a regular QTask
        self.execution_time = 0 # Targetted execution time. Only relevant if it is a cluster task.
        self.nb_requested_resources = 1 # 1 for regular instances, more if it is a cluster.

        self.datasets = list_datasets          # List of input datasets

        self.priority = int(priority)
        self.priority_group = PriorityGroup.fromValue(self.priority)
        self.user_id = user_id


    def print_infos(self, logger):
        logger.info("QTask: {}, {} received, {} waiting, {} dispatched, {} terminated, {} killed.".format(
            self.id, self.nb_received_instances, len(self.waiting_instances), self.nb_dispatched_instances, self.nb_terminated_instances, self.nb_killed_instances))

    def is_complete(self):
        #TODO make sure all instances of a task arrives at the same time in the workload
        return (len(self.waiting_instances) == 0) and (self.nb_received_instances == self.nb_terminated_instances)

    def instance_rejected(self, job):
        # This instance was rejected by the QBox it was dispatched to
        self.waiting_instances.append(job)
        self.nb_dispatched_instances -= 1

    def instance_submitted(self, job, resubmit=False):
        # An instance was submitted by Batsim (or re-submitted by the QNode)
        job.priority_group = self.priority_group
        self.waiting_instances.append(job)
        if not resubmit:
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
        self.nb_killed_instances += 1


    # Cluster-related functions
    def is_cluster(self):
        return self.is_cluster


class SubQTask:
    # Used by the QBox scheduler
    def __init__(self, id, priority_group, instances, list_datasets):
        self.id = id
        self.priority_group = priority_group
        self.waiting_instances = instances     # List of batsim jobs that are waiting to be started
        self.running_instances = []            # List of batsim jobs that are currently running
        self.waiting_datasets = []             # Datasets that are waiting to be on disk
        self.datasets = [] if list_datasets is None else list_datasets # List of input datasets
        self.is_cluster = False                # Whether it is a cluster instance or not

    def update_waiting_datasets(self, new_waiting_datasets):
        self.waiting_datasets = new_waiting_datasets

    def pop_waiting_instance(self):
        return self.waiting_instances.pop()

    def mark_running_instance(self, job):
        if job in self.waiting_instances:
            self.waiting_instances.remove(job)
        self.running_instances.append(job)

    def instance_finished(self, job):
        self.running_instances.remove(job)

    # Cluster-related functions
    def is_cluster(self):
        return self.is_cluster


class QMoboState:
    OFF, IDLE, RUNBKGD, LAUNCHING, RUNLOW, RUNHIGH = range(6)
    # When in OFF, the batsim host should be in the last pstate or marked as unavailable
    fromPriority = {
        PriorityGroup.BKGD : RUNBKGD,
        PriorityGroup.LOW  : RUNLOW,
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
        self.pset_mobos = ProcSet() # The ProcSet of all mobos
        self.temperature_master = -1 # The batid of the temperature_master mobo


class QMobo:
    def __init__(self, name, batid, max_pstate):
        self.name = name             # The name of the mobo
        self.batid = batid           # The Batsim id of the mobo
        self.pstate = 1              # The current power state of the mobo
        #TODO For now the initial pstate is 1 (because 0 is the turbo boost, disabled for now)
        self.max_pstate = max_pstate # The last power state (corresponds to the state OFF)
        self.state = QMoboState.OFF  # The state of the mobo
        self.running_job = -1        # The Job running on this mobo
        self.reserved_job = -1       # The Job that reserved this mobo

    def push_job(self, job):
        assert self.running_job == -1, "Job {} placed on mobo {} {} that was already executing job {}".format(self.running_job.id, self.batid, self.name, job.id)
        self.running_job = job
        self.state = QMoboState.LAUNCHING

    def pop_job(self):
        assert self.running_job != -1, "Pop_job asked on mobo {} {} but it is not running any job (current state is {})".format(self.batid, self.name, self.state)
        job = self.running_job
        self.running_job = -1
        self.state = QMoboState.IDLE

        return job

    def push_direct_job(self, job):
        assert self.running_job.qtask_id == job.qtask_id, "Direct restart of instance {} on mobo {} {} but previous instance is of different QTask ({})".format(job.id, self.batid, self.name, self.running_job.id)
        self.running_job = job

    def turn_off(self):
        assert self.running_job == -1, "Turning OFF mobo {} {} that is running {}.".format(self.batid, self.name, self.running_job.id)
        assert self.state != QMoboState.OFF, "Turning OFF mobo {} {} that is already OFF.".format(self.batid, self.name)
        self.pstate = self.max_pstate
        self.state = QMoboState.OFF

    def push_burn_job(self, job):
        assert self.running_job == -1, "Starting burn_job on mobo {} {} that is running {}".format(self.batid, self.name, self.running_job.id)
        job.priority_group = PriorityGroup.BKGD
        self.running_job = job
        self.state = QMoboState.RUNBKGD
        self.pstate = 0

    def launch_job(self):
        assert self.state == QMoboState.LAUNCHING, "Asked to launch a job but mobo {} {} is not in LAUNCHING state.".format(self.batid, self.name)
        assert self.running_job != -1, "Asked to launch job but no one was pushed on mobo {} {}.".format(self.batid, self.name)
        self.state = QMoboState.fromPriority[self.running_job.priority_group]
        self.pstate = 0

    def is_reserved(self):
        return self.reserved_job != -1

    def is_reserved_low(self):
        return (self.reserved_job != -1) and (self.reserved_job.priority_group == PriorityGroup.LOW)

    def is_reserved_high(self):
        return (self.reserved_job != -1) and (self.reserved_job.priority_group == PriorityGroup.HIGH)


class QUser:
    # Used to manage quotas, limits etc by user
    def __init__(self, id, max_running_instances):
        self.id = id
        self.max_running_instances = int(max_running_instances)

    def print_infos(self, logger):
        logger.info("QUser: {}, max running instances: {}".format(
            self.id, self.max_running_instances))
