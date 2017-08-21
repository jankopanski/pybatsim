from __future__ import print_function

from enum import Enum

import json
import sys

import redis

import zmq


class Batsim(object):

    DYNAMIC_JOB_PREFIX = "dynamic_job"
    DYNAMIC_PROFILE_PREFIX = "dynamic_profile"

    def __init__(self, scheduler,
                 validatingmachine=None,
                 socket_endpoint='tcp://*:28000', verbose=0,
                 handle_dynamic_notify=True):
        self.socket_endpoint = socket_endpoint
        self.verbose = verbose
        self.handle_dynamic_notify = handle_dynamic_notify

        self.jobs = dict()

        sys.setrecursionlimit(10000)

        if validatingmachine is None:
            self.scheduler = scheduler
        else:
            self.scheduler = validatingmachine(scheduler)

        # open connection
        self._context = zmq.Context()
        self._connection = self._context.socket(zmq.REP)
        if self.verbose > 0:
            print("[PYBATSIM]: binding to {addr}"
                    .format(addr=self.socket_endpoint), flush=True)
        self._connection.bind(self.socket_endpoint)

        # initialize some public attributes
        self.nb_jobs_received = 0
        self.nb_jobs_submitted = 0
        self.nb_jobs_killed = 0
        self.nb_jobs_rejected = 0
        self.nb_jobs_scheduled = 0
        self.nb_jobs_completed = 0
        self.nb_jobs_timeout = 0

        self.has_dynamic_job_submissions = False

        self.dynamic_id_counter = {}

        self.scheduler.bs = self
        # import pdb; pdb.set_trace()
        # Wait the "simulation starts" message to read the number of machines
        self._read_bat_msg()

        self.scheduler.onAfterBatsimInit()

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

    def start_jobs_continuous(self, allocs):
        """
        allocs should have the followinf format:
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
                        # FixMe do not send "[9]"
                        "alloc": " ".join(map(str, res[job.id]))
                }
            }
            )
            self.nb_jobs_scheduled += 1

    def reject_jobs(self, jobs):
        """Reject the given jobs."""
        for job in jobs:
            self._events_to_send.append({
                "timestamp": self.time(),
                "type": "REJECT_JOB",
                "data": {
                        "job_id": job.id,
                }
            })
            self.nb_jobs_rejected += 1

    def change_job_state(self, job, state, kill_reason=""):
        """Change the state of a job."""
        self._events_to_send.append({
            "timestamp": self.time(),
            "type": "CHANGE_JOB_STATE",
            "data": {
                    "job_id": job.id,
                    "job_state": state.name,
                    "kill_reason": kill_reason
            }
        })

    def kill_jobs(self, jobs):
        """Kill the given jobs."""

        self._events_to_send.append({
            "timestamp": self.time(),
            "type": "KILL_JOB",
            "data": {
                    "job_ids": [job.id for job in jobs],
            }
        })

    def submit_job(self, res, walltime, profile, profile_name=None,
            workload_name=None):
        if workload_name is None:
            workload_name=Batsim.DYNAMIC_JOB_PREFIX

        workload_name = workload_name.replace("!", "%")

        job_id = self.dynamic_id_counter.setdefault(workload_name, 0)
        self.dynamic_id_counter[workload_name] += 1

        full_job_id = "{}!{}".format(workload_name, job_id)

        if profile_name is None:
            profile_name = "{}!{}".format(
                    Batsim.DYNAMIC_PROFILE_PREFIX, full_job_id)

        msg = {
            "timestamp": self.time(),
            "type": "SUBMIT_JOB",
            "data": {
                    "job_id": full_job_id,
                    "job": {
                        "profile": profile_name,
                        "id": job_id,
                        "res": res,
                        "walltime": walltime,
                        "subtime": self.time(),
                    },
                    "profile": profile
            }
        }
        self._events_to_send.append(msg)
        self.nb_jobs_submitted += 1
        self.has_dynamic_job_submissions = True

        return full_job_id

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
            job = Job.from_json_dict(json_dict)
        return job

    def request_consumed_energy(self):
        self._events_to_send.append(
            {
                "timestamp": self.time(),
                "type": "QUERY_REQUEST",
                "data": {
                    "requests": {"consumed_energy": {}}
                }
            }
        )

    def do_next_event(self):
        return self._read_bat_msg()

    def start(self):
        cont = True
        while cont:
            cont = self.do_next_event()

    def _read_bat_msg(self):
        msg = json.loads(self._connection.recv().decode('utf-8'))

        if self.verbose > 0:
            print('[PYBATSIM]: BATSIM ---> DECISION\n {}'.format(
                json.dumps(msg, indent=2)), flush=True)

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
                self.nb_res = event_data["nb_resources"]
                batconf = event_data["config"]
                self.time_sharing = event_data["allow_time_sharing"]

                self.redis_enabled = batconf["redis"]["enabled"]
                redis_hostname = batconf["redis"]["hostname"]
                redis_port = batconf["redis"]["port"]
                redis_prefix = batconf["redis"]["prefix"]

                if self.redis_enabled:
                    self.redis = DataStorage(redis_prefix, redis_hostname,
                                             redis_port)

                self.resources = event_data["resources_data"]

                self.hpst = event_data.get("hpst_host", None)
                self.lcst = event_data.get("lcst_host", None)
                self.scheduler.onSimulationBegins()

            elif event_type == "SIMULATION_ENDS":
                print("[PYBATSIM]: All jobs have been submitted and completed!",
                        flush=True)
                finished_received = True
                self.scheduler.onSimulationEnds()
            elif event_type == "JOB_SUBMITTED":
                # Received WORKLOAD_NAME!JOB_ID
                job_id = event_data["job_id"]
                self.jobs[job_id] = self.get_job(event)
                self.scheduler.onJobSubmission(self.jobs[job_id])
                self.nb_jobs_received += 1
            elif event_type == "JOB_KILLED":
                self.scheduler.onJobsKilled([self.jobs[jid] for jid in event_data["job_ids"]])
                self.nb_jobs_killed += len(event_data["job_ids"])
            elif event_type == "JOB_COMPLETED":
                job_id = event_data["job_id"]
                j = self.jobs[job_id]
                j.finish_time = event["timestamp"]
                j.status = event["data"]["status"]

                try:
                    j.job_state = Job.State[event["data"]["job_state"]]
                except KeyError:
                    j.job_state = Job.State.UNKNOWN
                j.kill_reason = event["data"]["kill_reason"]

                self.scheduler.onJobCompletion(j)
                if j.status == "TIMEOUT":
                    self.nb_jobs_timeout += 1
                else:
                    self.nb_jobs_completed += 1
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
            elif event_type == "QUERY_REPLY":
                consumed_energy = event_data["consumed_energy"]
                self.scheduler.onReportEnergyConsumed(consumed_energy)
            elif event_type == 'REQUESTED_CALL':
                self.scheduler.onNOP()
                # TODO: separate NOP / REQUESTED_CALL (here and in the algos)
            else:
                raise Exception("Unknow event type {}".format(event_type))

        if self.handle_dynamic_notify and not finished_received:
            if ((self.nb_jobs_completed + self.nb_jobs_timeout + self.nb_jobs_killed) == self.nb_jobs_scheduled
                    and not self.has_dynamic_job_submissions):
                self.notify_submission_finished()
            else:
                self.notify_submission_continue()
                self.has_dynamic_job_submissions = False

        if len(self._events_to_send) > 0:
            # sort msgs by timestamp
            self._events_to_send = sorted(
                self._events_to_send, key=lambda event: event['timestamp'])

        new_msg = {
            "now": self._current_time,
            "events": self._events_to_send
        }
        if self.verbose > 0:
            print("[PYBATSIM]: BATSIM ---> DECISION\n {}".format(new_msg),
                    flush=True)
        self._connection.send_string(json.dumps(new_msg))

        if finished_received:
            self._connection.close()

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
        key = 'job_{job_id}'.format(job_id=job_id)
        job_str = self.get(key).decode('utf-8')

        return Job.from_json_string(job_str)

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
        COMPLETED_KILLED = 4
        REJECTED = 5

    def __init__(self, id, subtime, walltime, res, profile, json_dict):
        self.id = id
        self.submit_time = subtime
        self.requested_time = walltime
        self.requested_resources = res
        self.profile = profile
        self.finish_time = None  # will be set on completion by batsim
        self.status = None
        self.job_state = Job.State.UNKNOWN
        self.kill_reason = None
        self.json_dict = json_dict

    def __repr__(self):
        return("<Job {0}; sub:{1} res:{2} reqtime:{3} prof:{4} stat:{5} jstat:{6} kill:{7}>".format(
            self.id, self.submit_time, self.requested_resources,
            self.requested_time, self.profile, self.status,
            self.job_state, self.kill_reason))

    @staticmethod
    def from_json_string(json_str):
        json_dict = json.loads(json_str)
        return Job.from_json_dict(json_dict)

    @staticmethod
    def from_json_dict(json_dict):
        return Job(json_dict["id"],
                   json_dict["subtime"],
                   json_dict["walltime"],
                   json_dict["res"],
                   json_dict["profile"],
                   json_dict)
    # def __eq__(self, other):
        # return self.id == other.id
    # def __ne__(self, other):
        # return not self.__eq__(other)


class BatsimScheduler(object):

    def __init__(self, options):
        self.options = options

    def onAfterBatsimInit(self):
        # You now have access to self.bs and all other functions
        pass

    def onSimulationBegins(self):
        pass

    def onSimulationEnds(self):
        pass

    def onNOP(self):
        raise NotImplementedError()

    def onJobSubmission(self, job):
        raise NotImplementedError()

    def onJobCompletion(self, job):
        raise NotImplementedError()

    def onJobsKilled(self, jobs):
        for job in jobs:
            self.onJobCompletion(job)

    def onMachinePStateChanged(self, nodeid, pstate):
        raise NotImplementedError()

    def onReportEnergyConsumed(self, consumed_energy):
        raise NotImplementedError()
