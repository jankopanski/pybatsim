from __future__ import print_function

import json
import sys

import redis

import zmq


class Batsim(object):

    def __init__(self, scheduler,
                 validatingmachine=None,
                 socket_endpoint='tcp://*:28000', verbose=0):
        self.socket_endpoint = socket_endpoint
        self.verbose = verbose

        self.jobs = dict()

        sys.setrecursionlimit(10000)

        if validatingmachine is None:
            self.scheduler = scheduler
        else:
            self.scheduler = validatingmachine(scheduler)

        # open connection
        self._context = zmq.Context()
        self._connection = self._context.socket(zmq.REP)
        print("[BATSIM]: binding to {addr}".format(addr=self.socket_endpoint))
        self._connection.bind(self.socket_endpoint)

        # initialize some public attributes
        self.nb_jobs_received = 0
        self.nb_jobs_scheduled = 0

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

    # def change_pstates(self, pstates_to_change):
    #     """
    #     should be [ (new_pstate, (first_node, last_node)),  ...]
    #     """
    #     if len(pstates_to_change) == 0:
    #         return

    #     parts = []
    #     for (new_pstate, (first_res, last_res)) in pstates_to_change:
    #         if first_res == last_res:
    #             parts.append(str(first_res) + "=" + str(new_pstate))
    #         else:
    #             parts.append(str(first_res) + "-" + str(last_res) +
    #                          "=" + str(new_pstate))
    #     for part in parts:
    #         self._events_to_send.append((self.time(), "P:" + part))

    # def change_pstate_merge(self, new_pstate, first_node, last_node):
    #     """
    #     if the previous call of change_pstate_merge had the same new_pstate
    #     and old.last_node+1 == new.first_node, then we merge the requests.
    #     """
    #     part = None
    #     if len(self._events_to_send) > 0:
    #         last_msg = self._events_to_send[-1]
    #         if last_msg[0] == self.time():
    #             resInter = re.split(
    #                 "P:([0-9]+)-([0-9]+)=([0-9]+)", last_msg[1])
    #             resUniq = re.split("P:([0-9]+)=([0-9]+)", last_msg[1])
    #             if (len(resInter) == 5
    #                     and int(resInter[3]) == new_pstate
    #                     and int(resInter[2]) + 1 == first_node):
    #                 self._events_to_send.pop(-1)
    #                 part = str(resInter[1]) + "-" + \
    #                     str(last_node) + "=" + str(new_pstate)
    #             elif (len(resUniq) == 4
    #                     and int(resUniq[2]) == new_pstate
    #                     and int(resUniq[1]) + 1 == first_node):
    #                 self._events_to_send.pop(-1)
    #                 part = str(resUniq[1]) + "-" + \
    #                     str(last_node) + "=" + str(new_pstate)
    #     if part is None:
    #         part = str(first_node) + "-" + \
    #             str(last_node) + "=" + str(new_pstate)

    #     self._events_to_send.append((self.time(), "P:" + part))

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
                json.dumps(msg, ident=2))
            )

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

                self.redis_enabled = batconf["redis"]["enabled"]
                redis_hostname = batconf["redis"]["hostname"]
                redis_port = batconf["redis"]["port"]
                redis_prefix = batconf["redis"]["prefix"]

                if self.redis_enabled:
                    self.redis = DataStorage(redis_prefix, redis_hostname,
                                             redis_port)

            elif event_type == "SIMULATION_ENDS":
                print("All jobs have been submitted and completed!")
                finished_received = True
            elif event_type == "JOB_SUBMITTED":
                # Received WORKLOAD_NAME!JOB_ID
                job_id = event_data["job_id"]
                self.jobs[job_id] = self.get_job(event)
                self.scheduler.onJobSubmission(self.jobs[job_id])
                self.nb_jobs_received += 1
            elif event_type == "JOB_COMPLETED":
                job_id = event_data["job_id"]
                j = self.jobs[job_id]
                j.finish_time = event["timestamp"]
                self.scheduler.onJobCompletion(j)
            elif event_type == "RESOURCE_STATE_CHANGED":
                nodes = event_data["resources"].split("-")
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

        if len(self._events_to_send) > 0:
            # sort msgs by timestamp
            self._events_to_send = sorted(
                self._events_to_send, key=lambda event: event['timestamp'])

        new_msg = {
            "now": self._current_time,
            "events": self._events_to_send
        }
        if self.verbose > 0:
            print("[PYBATSIM]: BATSIM ---> DECISION\n {}".format(new_msg))
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

    def __init__(self, id, subtime, walltime, res, profile):
        self.id = id
        self.submit_time = subtime
        self.requested_time = walltime
        self.requested_resources = res
        self.profile = profile
        self.finish_time = None  # will be set on completion by batsim

    def __repr__(self):
        return("<Job {0}; sub:{1} res:{2} reqtime:{3} prof:{4}>".format(
            self.id, self.submit_time, self.requested_resources,
            self.requested_time, self.profile))

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
                   json_dict["profile"])
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

    def onNOP(self):
        raise Exception("not implemented")

    def onJobSubmission(self, job):
        raise Exception("not implemented")

    def onJobCompletion(self, job):
        raise Exception("not implemented")

    def onMachinePStateChanged(self, nodeid, pstate):
        raise Exception("not implemented")

    def onReportEnergyConsumed(self, consumed_energy):
        raise Exception("not implemented")
