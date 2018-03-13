# from __future__ import print_function

from enum import Enum
from copy import deepcopy

import json
import sys

from .network import NetworkHandler

from procset import ProcSet
import redis
import zmq
import logging



class Batsim(object):

    WORKLOAD_JOB_SEPARATOR = "!"
    ATTEMPT_JOB_SEPARATOR = "#"
    WORKLOAD_JOB_SEPARATOR_REPLACEMENT = "%"

    def __init__(self, scheduler,
                 network_handler=None,
                 event_handler=None,
                 validatingmachine=None):


        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=FORMAT)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        self.running_simulation = False
        if network_handler is None:
            network_handler = NetworkHandler('tcp://*:28000')
        if event_handler is None:
            event_handler = NetworkHandler(
                'tcp://127.0.0.1:28001', type=zmq.PUB)
        self.network = network_handler
        self.event_publisher = event_handler

        self.jobs = dict()

        sys.setrecursionlimit(10000)

        if validatingmachine is None:
            self.scheduler = scheduler
        else:
            self.scheduler = validatingmachine(scheduler)

        # initialize some public attributes
        self.nb_jobs_received = 0
        self.nb_jobs_submitted = 0
        self.nb_jobs_killed = 0
        self.nb_jobs_rejected = 0
        self.nb_jobs_scheduled = 0
        self.nb_jobs_completed = 0
        self.nb_jobs_successful = 0
        self.nb_jobs_failed = 0
        self.nb_jobs_timeout = 0

        self.jobs_manually_changed = set()

        self.has_dynamic_job_submissions = False

        self.network.bind()
        self.event_publisher.bind()

        self.scheduler.bs = self
        # import pdb; pdb.set_trace()
        # Wait the "simulation starts" message to read the number of machines
        self._read_bat_msg()

        self.scheduler.onAfterBatsimInit()

    def publish_event(self, event):
        """Sends a message to subscribed event listeners (e.g. external processes which want to
        observe the simulation).
        """
        self.event_publisher.send_string(event)

    def time(self):
        return self._current_time

    def consume_time(self, t):
        self._current_time += float(t)
        return self._current_time

    def wake_me_up_at(self, time):
        self._events_to_send.append(
            {"timestamp": self.time(),
             "type": "CALL_ME_LATER",
             "data": {"timestamp": time}})

    def notify_submission_finished(self):
        self._events_to_send.append({
            "timestamp": self.time(),
            "type": "NOTIFY",
            "data": {
                    "type": "submission_finished",
            }
        })

    def notify_submission_continue(self):
        self._events_to_send.append({
            "timestamp": self.time(),
            "type": "NOTIFY",
            "data": {
                    "type": "continue_submission",
            }
        })

    def send_message_to_job(self, job, message):
        self._events_to_send.append({
            "timestamp": self.time(),
            "type": "TO_JOB_MSG",
            "data": {
                    "job_id": job.id,
                    "msg": message,
            }
        })

    def start_jobs_continuous(self, allocs):
        """
        allocs should have the following format:
        [ (job, (first res, last res)), (job, (first res, last res)), ...]
        """

        if len(allocs) == 0:
            return

        for (job, (first_res, last_res)) in allocs:
            self._events_to_send.append({
                "timestamp": self.time(),
                "type": "EXECUTE_JOB",
                "data": {
                        "job_id": job.id,
                        "alloc": "{}-{}".format(first_res, last_res)
                }
            }
            )
            self.nb_jobs_scheduled += 1

    def start_jobs(self, jobs, res):
        """ args:res: is list of int (resources ids) """
        for job in jobs:
            self._events_to_send.append({
                "timestamp": self.time(),
                "type": "EXECUTE_JOB",
                "data": {
                        "job_id": job.id,
                        "alloc": str(ProcSet(*res[job.id]))
                }
            }
            )
            self.nb_jobs_scheduled += 1

    def execute_jobs(self, jobs):
        """ args:jobs: list of jobs to execute (job.allocation MUST be set) """

        for job in jobs:
            assert job.allocation is not None
            self._events_to_send.append({
                "timestamp": self.time(),
                "type": "EXECUTE_JOB",
                "data": {
                        "job_id": job.id,
                        "alloc": str(job.allocation)
                }
            }
            )
            self.nb_jobs_scheduled += 1


    def reject_jobs(self, jobs):
        """Reject the given jobs."""
        assert len(jobs) > 0, "The list of jobs to reject is empty"
        for job in jobs:
            self._events_to_send.append({
                "timestamp": self.time(),
                "type": "REJECT_JOB",
                "data": {
                        "job_id": job.id,
                }
            })
            self.nb_jobs_rejected += 1

    def change_job_state(self, job, state):
        """Change the state of a job."""
        self._events_to_send.append({
            "timestamp": self.time(),
            "type": "CHANGE_JOB_STATE",
            "data": {
                    "job_id": job.id,
                    "job_state": state.name,
            }
        })
        self.jobs_manually_changed.add(job)

    def kill_jobs(self, jobs):
        """Kill the given jobs."""
        assert len(jobs) > 0, "The list of jobs to kill is empty"
        for job in jobs:
            job.job_state = Job.State.IN_KILLING
        self._events_to_send.append({
            "timestamp": self.time(),
            "type": "KILL_JOB",
            "data": {
                    "job_ids": [job.id for job in jobs],
            }
        })

    def submit_profiles(self, workload_name, profiles):
        for profile_name, profile in profiles.items():
            msg = {
                "timestamp": self.time(),
                "type": "SUBMIT_PROFILE",
                "data": {
                    "workload_name": workload_name,
                    "profile_name": profile_name,
                    "profile": profile,
                }
            }
            self._events_to_send.append(msg)

    def submit_job(
            self,
            id,
            res,
            walltime,
            profile_name,
            subtime=None,
            profile=None):

        if subtime is None:
            subtime = self.time()
        job_dict = {
            "profile": profile_name,
            "id": id,
            "res": res,
            "walltime": walltime,
            "subtime": subtime,
        }
        msg = {
            "timestamp": self.time(),
            "type": "SUBMIT_JOB",
            "data": {
                "job_id": id,
                "job": job_dict,
            }
        }
        if profile is not None:
            assert isinstance(profile, dict)
            msg["data"]["profile"] = profile

        self._events_to_send.append(msg)
        self.nb_jobs_submitted += 1

        self.has_dynamic_job_submissions = True

        # Create the job here
        self.jobs[id] = Job.from_json_dict(job_dict, profile_dict=profile)
        self.jobs[id].job_state = Job.State.SUBMITTED

        return id

    def set_resource_state(self, resources, state):
        """ args:resources: is a list of resource numbers or intervals as strings (e.g. "1-5").
            args:state: is a state identifier configured in the platform specification.
        """

        self._events_to_send.append({
            "timestamp": self.time(),
            "type": "SET_RESOURCE_STATE",
            "data": {
                    "resources": " ".join([str(r) for r in resources]),
                    "state": str(state)
            }
        })

    def start_jobs_interval_set_strings(self, jobs, res):
        """ args:res: is a jobID:interval_set_string dict """
        for job in jobs:
            self._events_to_send.append({
                "timestamp": self.time(),
                "type": "EXECUTE_JOB",
                "data": {
                        "job_id": job.id,
                        "alloc": res[job.id]
                }
            }
            )
            self.nb_jobs_scheduled += 1

    def get_job(self, event):
        if self.redis_enabled:
            job = self.redis.get_job(event["data"]["job_id"])
        else:
            json_dict = event["data"]["job"]
            try:
                profile_dict = event["data"]["profile"]
            except KeyError:
                profile_dict = {}
            job = Job.from_json_dict(json_dict, profile_dict)
        return job

    def request_consumed_energy(self):
        self._events_to_send.append(
            {
                "timestamp": self.time(),
                "type": "QUERY",
                "data": {
                    "requests": {"consumed_energy": {}}
                }
            }
        )

    def notify_resources_added(self, resources):
        self._events_to_send.append(
            {
                "timestamp": self.time(),
                "type": "RESOURCES_ADDED",
                "data": {
                    "resources": resources
                }
            }
        )

    def notify_resources_removed(self, resources):
        self._events_to_send.append(
            {
                "timestamp": self.time(),
                "type": "RESOURCES_REMOVED",
                "data": {
                    "resources": resources
                }
            }
        )

    def set_job_metadata(self, job_id, metadata):
        # Consume some time to be sur that the job was created before the
        # metadata is set

        self.jobs[job_id].metadata = metadata
        self._events_to_send.append(
            {
                "timestamp": self.time(),
                "type": "SET_JOB_METADATA",
                "data": {
                    "job_id": str(job_id),
                    "metadata": str(metadata)
                }
            }
        )

    def resubmit_job(self, job):
        """
        The given job is resubmited but in a dynamic workload. The name of this
        workload is "resubmit=N" where N is the number of resubmission.
        The job metadata is fill with a dict that contains the original job
        full id in "parent_job" and the number of resubmission in "nb_resumit".

        Warning: The profile_dict of the given job must be filled
        """

        if job.metadata is None:
            metadata = {"parent_job" : job.id, "nb_resubmit": 1}
        else:
            metadata = deepcopy(job.metadata)
            if "parent_job" not in metadata:
                metadata["parent_job"] = job.id
            metadata["nb_resubmit"] = metadata["nb_resubmit"] + 1

        # Keep the curent workload and add a resubmit number
        splitted_id = job.id.split(Batsim.ATTEMPT_JOB_SEPARATOR)
        if len(splitted_id) == 0:
            new_job_name = job.id
        else:
            # This job as already an attempt number
            new_job_name = splitted_id[0]
        new_job_name =  new_job_name + Batsim.ATTEMPT_JOB_SEPARATOR + str(metadata["nb_resubmit"])

        new_job_id = self.submit_job(
                new_job_name,
                job.requested_resources,
                job.requested_time,
                job.profile,
                profile=job.profile_dict)

        # log in job metadata parent job and nb resubmit
        self.set_job_metadata(new_job_id, metadata)

    def do_next_event(self):
        return self._read_bat_msg()

    def start(self):
        cont = True
        while cont:
            cont = self.do_next_event()

    def _read_bat_msg(self):
        msg = None
        while msg is None:
            msg = self.network.recv(blocking=not self.running_simulation)
            if msg is None:
                self.scheduler.onDeadlock()
                continue
        self.logger.info("Message Received from Batsim: {}".format(msg))

        self._current_time = msg["now"]

        if msg["events"] is []:
            # No events in the message
            self.scheduler.onNOP()

        self._events_to_send = []

        finished_received = False

        for event in msg["events"]:
            event_type = event["type"]
            event_data = event.get("data", {})
            if event_type == "SIMULATION_BEGINS":
                assert not self.running_simulation, "A simulation is already running (is more than one instance of Batsim active?!)"
                self.running_simulation = True
                self.nb_res = event_data["nb_resources"]
                self.batconf = event_data["config"]
                self.time_sharing = event_data["allow_time_sharing"]
                self.handle_dynamic_notify = self.batconf["job_submission"]["from_scheduler"]["enabled"]

                self.redis_enabled = self.batconf["redis"]["enabled"]
                redis_hostname = self.batconf["redis"]["hostname"]
                redis_port = self.batconf["redis"]["port"]
                redis_prefix = self.batconf["redis"]["prefix"]

                if self.redis_enabled:
                    self.redis = DataStorage(redis_prefix, redis_hostname,
                                             redis_port)

                # Retro compatibility for old Batsim API > 1.0 < 3.0
                if "resources_data" in event_data:
                    res_key = "resources_data"
                else:
                    res_key = "compute_resources"
                self.resources = {
                        res["id"]: res for res in event_data[res_key]}

                self.hpst = event_data.get("hpst_host", None)
                self.lcst = event_data.get("lcst_host", None)
                self.scheduler.onSimulationBegins()

            elif event_type == "SIMULATION_ENDS":
                assert self.running_simulation, "No simulation is currently running"
                self.running_simulation = False
                self.logger.info("All jobs have been submitted and completed!")
                finished_received = True
                self.scheduler.onSimulationEnds()
            elif event_type == "JOB_SUBMITTED":
                # Received WORKLOAD_NAME!JOB_ID
                job_id = event_data["job_id"]
                job = self.get_job(event)
                job.job_state = Job.State.SUBMITTED

                # don't override dynamic job
                if job_id not in self.jobs:
                    self.jobs[job_id] = job

                self.scheduler.onJobSubmission(job)
                self.nb_jobs_received += 1
            elif event_type == "JOB_KILLED":
                # get progress
                killed_jobs = []
                for jid in event_data["job_ids"]:
                    j = self.jobs[jid]
                    j.progress = event_data["job_progress"][jid]
                    killed_jobs.append(j)
                self.scheduler.onJobsKilled(killed_jobs)
            elif event_type == "JOB_COMPLETED":
                job_id = event_data["job_id"]
                j = self.jobs[job_id]
                j.finish_time = event["timestamp"]

                try:
                    j.job_state = Job.State[event["data"]["job_state"]]
                except KeyError:
                    j.job_state = Job.State.UNKNOWN
                j.return_code = event["data"]["return_code"]

                self.scheduler.onJobCompletion(j)
                if j.job_state == Job.State.COMPLETED_WALLTIME_REACHED:
                    self.nb_jobs_timeout += 1
                elif j.job_state == Job.State.COMPLETED_FAILED:
                    self.nb_jobs_failed += 1
                elif j.job_state == Job.State.COMPLETED_SUCCESSFULLY:
                    self.nb_jobs_successful += 1
                elif j.job_state == Job.State.COMPLETED_KILLED:
                    self.nb_jobs_killed += 1
                self.nb_jobs_completed += 1
            elif event_type == "FROM_JOB_MSG":
                job_id = event_data["job_id"]
                j = self.jobs[job_id]
                timestamp = event["timestamp"]
                msg = event_data["msg"]
                self.scheduler.onJobMessage(timestamp, j, msg)
            elif event_type == "RESOURCE_STATE_CHANGED":
                intervals = event_data["resources"].split(" ")
                for interval in intervals:
                    nodes = interval.split("-")
                    if len(nodes) == 1:
                        nodeInterval = (int(nodes[0]), int(nodes[0]))
                    elif len(nodes) == 2:
                        nodeInterval = (int(nodes[0]), int(nodes[1]))
                    else:
                        raise Exception("Multiple intervals are not supported")
                    self.scheduler.onMachinePStateChanged(
                        nodeInterval, event_data["state"])
            elif event_type == "ANSWER":
                consumed_energy = event_data["consumed_energy"]
                self.scheduler.onReportEnergyConsumed(consumed_energy)
            elif event_type == 'REQUESTED_CALL':
                self.scheduler.onNOP()
                # TODO: separate NOP / REQUESTED_CALL (here and in the algos)
            elif event_type == 'ADD_RESOURCES':
                self.scheduler.onAddResources(event_data["resources"])
            elif event_type == 'REMOVE_RESOURCES':
                self.scheduler.onRemoveResources(event_data["resources"])
            else:
                raise Exception("Unknow event type {}".format(event_type))

        self.scheduler.onNoMoreEvents()

        if self.handle_dynamic_notify and not finished_received:
            if (self.nb_jobs_completed == self.nb_jobs_received != 0):
                # All the received and submited jobs are completed or killed
                self.notify_submission_finished()
            else:
                #self.notify_submission_continue()
                # Some jobs just have been dynamically submitted
                self.has_dynamic_job_submissions = False

        if len(self._events_to_send) > 0:
            # sort msgs by timestamp
            self._events_to_send = sorted(
                self._events_to_send, key=lambda event: event['timestamp'])

        new_msg = {
            "now": self._current_time,
            "events": self._events_to_send
        }
        self.network.send(new_msg)
        self.logger.info("Message Sent to Batsim: {}".format(new_msg))


        if finished_received:
            self.network.close()
            self.event_publisher.close()
            if self.handle_dynamic_notify:
                self.notify_submission_finished()

        return not finished_received


class DataStorage(object):
    ''' High-level access to the Redis data storage system '''

    def __init__(self, prefix, hostname='localhost', port=6379):
        self.prefix = prefix
        self.redis = redis.StrictRedis(host=hostname, port=port)

    def get(self, key):
        real_key = '{iprefix}:{ukey}'.format(iprefix=self.prefix,
                                             ukey=key)
        value = self.redis.get(real_key)
        assert(value is not None), "Redis: No such key '{k}'".format(
            k=real_key)
        return value

    def get_job(self, job_id):
        job_key = 'job_{job_id}'.format(job_id=job_id)
        job_str = self.get(job_key).decode('utf-8')
        job = Job.from_json_string(job_str)

        profile_key = 'profile_{workload_id}!{profile_id}'.format(
            workload_id=job_id.split(Batsim.WORKLOAD_JOB_SEPARATOR)[0],
            profile_id=job.profile)
        profile_str = self.get(profile_key).decode('utf-8')
        job.profile_dict = json.loads(profile_str)

        return job

    def set_job(self, job_id, subtime, walltime, res):
        real_key = '{iprefix}:{ukey}'.format(iprefix=self.prefix,
                                             ukey=job_id)
        json_job = json.dumps({"id": job_id, "subtime": subtime,
                               "walltime": walltime, "res": res})
        self.redis.set(real_key, json_job)


class Job(object):

    class State(Enum):
        UNKNOWN = -1
        NOT_SUBMITTED = 0
        SUBMITTED = 1
        RUNNING = 2
        COMPLETED_SUCCESSFULLY = 3
        COMPLETED_FAILED = 4
        COMPLETED_WALLTIME_REACHED = 5
        COMPLETED_KILLED = 6
        REJECTED = 7
        IN_KILLING = 8

    def __init__(
            self,
            id,
            subtime,
            walltime,
            res,
            profile,
            json_dict,
            profile_dict):
        self.id = id
        self.submit_time = subtime
        self.requested_time = walltime
        self.requested_resources = res
        self.profile = profile
        self.finish_time = None  # will be set on completion by batsim
        self.job_state = Job.State.UNKNOWN
        self.return_code = None
        self.progress = None
        self.json_dict = json_dict
        self.profile_dict = profile_dict
        self.allocation = None
        self.metadata = None

    def __repr__(self):
        return(
            ("<Job {0}; sub:{1} res:{2} reqtime:{3} prof:{4} "
                "state:{5} ret:{6} alloc:{7}>\n").format(
            self.id, self.submit_time, self.requested_resources,
            self.requested_time, self.profile,
            self.job_state,
            self.return_code, self.allocation))

    @staticmethod
    def from_json_string(json_str):
        json_dict = json.loads(json_str)
        return Job.from_json_dict(json_dict)

    @staticmethod
    def from_json_dict(json_dict, profile_dict=None):
        return Job(json_dict["id"],
                   json_dict["subtime"],
                   json_dict.get("walltime", -1),
                   json_dict["res"],
                   json_dict["profile"],
                   json_dict,
                   profile_dict)
    # def __eq__(self, other):
        # return self.id == other.id
    # def __ne__(self, other):
        # return not self.__eq__(other)


class BatsimScheduler(object):

    def __init__(self, options):
        self.options = options

        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=FORMAT)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

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

    def onNOP(self):
        raise NotImplementedError()

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

    def onNoMoreEvents(self):
        pass
