from batsim.batsim import Job as BatsimJob


class Job:

    def __init__(self, batsim_job=None):
        self._batsim_job = batsim_job
        self._scheduled = False
        self._submitted = False
        self._rejected = False
        self._rejected_reason = None
        self._reservation = []
        self._previous_reservations = []

    def free_all(self):
        self._previous_reservations.append(self._reservation[:])
        for res in self._reservation[:]:
            self.free(res)

    def free(self, resource, recursive_call=False):
        # To free resources the job does either have to be not submitted yet
        # or the job has to be completed (i.e. the job status is set by
        # batsim).
        assert not self._submitted or self._batsim_job.status in [
            "SUCCESS", "TIMEOUT"]

        if not recursive_call:
            resource.free(self, recursive_call=True)
        self._reservation.remove(resource)

    def reserve(self, resource, recursive_call=False):
        if not recursive_call:
            resource.allocate(self, recursive_call=True)
        self._reservation += resource.resources

    @property
    def scheduled(self):
        return self._scheduled

    @property
    def rejected(self):
        return self._rejected

    @property
    def reservation(self):
        return tuple(self._reservation)

    @property
    def submitted(self):
        return self._submitted

    @property
    def previous_reservations(self):
        return tuple(self._previous_reservations)

    def schedule(self, resource=None):
        if resource:
            self.reserve(resource)

        assert not self.submitted
        self._scheduled = True

    def reject(self, reason=""):
        assert not self.submitted and not self.scheduled
        self._rejected = True
        self._rejected_reason = reason

    def _do_reject(self, scheduler):
        scheduler.info(
            "Rejecting job ({}), reason={}".format(
                self, self._rejected_reason))
        scheduler._batsim.reject_jobs([self._batsim_job])
        scheduler._open_jobs.remove(self)
        scheduler._rejected_jobs.append(self)
        del scheduler._job_map[self._batsim_job.id]

    def submit(self, scheduler):
        assert self._batsim_job is not None
        assert not self.submitted and not self.rejected

        if not self.scheduled:
            self.schedule()

        if self._batsim_job.requested_resources < len(self._reservation):
            scheduler.warn(
                "Starting of job ({}) is postponed since not enough resources are allocated" .format(self))
            return

        self._submitted = True

        alloc = []
        for res in self._reservation[:self._batsim_job.requested_resources]:
            alloc.append(res.id)
            res.computing = True

        scheduler._batsim.start_jobs(
            [self._batsim_job], {self._batsim_job.id: alloc})

        self._scheduled = False

        scheduler._open_jobs.remove(self)
        scheduler._scheduled_jobs.append(self)

        scheduler.info("Starting job ({})".format(self))

    def __str__(self):
        return str(self._batsim_job)

    @property
    def id(self):
        return self._batsim_job.id

    @property
    def submit_time(self):
        return self._batsim_job.submit_time

    @property
    def requested_time(self):
        return self._batsim_job.requested_time

    @property
    def requested_resources(self):
        return self._batsim_job.requested_resources

    @property
    def profile(self):
        return self._batsim_job.profile

    @property
    def finish_time(self):
        return self._batsim_job.finish_time

    @property
    def status(self):
        return self._batsim_job.status

    @property
    def job_state(self):
        return self._batsim_job.job_state

    @property
    def kill_reason(self):
        return self._batsim_job.kill_reason
