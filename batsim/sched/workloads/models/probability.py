"""
    batsim.sched.workloads.models.probability
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This is a simple probability model for different job classes.

    The job classes can be modified by subclassing `ProbabilityModel` and
    overriding the method `generate_job_classes`.
"""

from batsim.sched.workloads.models.generator import WorkloadModel, \
    JobModelData, ParamRange, Option


class ProbabilityModel(WorkloadModel):

    @property
    def default_parameters(self):
        params = {
            "exact_delay": Option(
                "Whether the jobs should run exactly for their walltime",
                False),
            "include_each_class": Option(
                "Whether each job class should at least occur once",
                True),
            "submit_factor": Option(
                "The factor applied to submission times",
                0.1),
        }
        return self.merge_parameters(params, super().default_parameters)

    def jobclass_to_job(
            self,
            last_submit_time,
            idx,
            random,
            jobclass,
            parameters):
        jobobj = jobclass.copy_job()

        if parameters["exact_delay"]:
            jobobj.run_time = None

        jobobj.submit_time = random.uniform(
            last_submit_time,
            last_submit_time + idx * parameters["submit_factor"])

        jobobj.process(random)

        return jobobj

    def generate_job_classes(self, parameters):
        """Returns a list of job classes.

        The list contains tuples wheras the first element of the tuple is the
        probability for the model to occur and the second element is a `JobModelData`
        object describing the job type.
        """
        num_machines = int(parameters["num_machines"])
        return [
            (0.05, JobModelData(
                requested_processors=max(1, num_machines),
                requested_time=400,
                run_time=ParamRange(350, 390))),

            (0.20, JobModelData(
                requested_processors=max(1, num_machines // 2),
                requested_time=250,
                run_time=ParamRange(150, 245))),

            (0.60, JobModelData(
                requested_processors=max(1, num_machines // 4),
                requested_time=80,
                run_time=ParamRange(35, 55))),

            (0.80, JobModelData(
                requested_processors=max(1, num_machines // 8),
                requested_time=90,
                run_time=ParamRange(75, 88))),

            (1.00, JobModelData(
                requested_processors=max(1, num_machines // 16),
                requested_time=40,
                run_time=ParamRange(28, 32))),
        ]

    def create_jobs(self, random, parameters):
        classes = self.generate_job_classes(parameters)
        generated_types = []

        if parameters["include_each_class"]:
            for _, cls in classes:
                generated_types.append(cls.copy_job())

        while len(generated_types) < int(parameters["num_jobs"]):
            generated_types.append(random.choices(
                [c[1] for c in classes],
                [c[0] for c in classes])[0])

        random.shuffle(generated_types)
        generated_types = generated_types[:int(parameters["num_jobs"])]

        last_submit_time = 0
        idx = 0
        jobs = []
        for t in generated_types:
            jobobj = self.jobclass_to_job(
                last_submit_time, idx, random, t, parameters)
            jobs.append(jobobj)
            last_submit_time = jobobj.submit_time
            idx += 1

        return jobs


if __name__ == "__main__":
    ProbabilityModel.as_main()
