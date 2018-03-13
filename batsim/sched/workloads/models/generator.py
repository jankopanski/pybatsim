"""
    batsim.sched.workloads.models.generator
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module contains base classes and declarations which can be extended to implement
    concrete workload models which directly work with Batsim.
"""
import argparse
import random
import sys
import time
import copy
import json

from .. import WorkloadDescription
from batsim.sched.profiles import Profiles

from ...utils import increment_float


class ParamRange:

    def __init__(self, lower_bound, upper_bound):
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound

    def __call__(self, random_instance):
        is_float = isinstance(self.lower_bound, float) or \
            isinstance(self.upper_bound, float)
        randf = random_instance.uniform if is_float else random_instance.randint
        return randf(self.lower_bound, self.upper_bound)


class JobModelData:

    def __init__(self, **kwargs):
        # Fields defined in Standard Workload Format version 2.2

        # Job id (optional)
        self.job_number = None

        # Submission time (optional)
        self.submit_time = None

        # Difference between the job's submit time and the start time
        # (Not relevant to workloads)
        self.wait_time = None

        # End time minus start time
        # (Not relevant to workloads)
        self.run_time = None

        # Number of processors used by the job
        # (Not relevant to workloads)
        self.used_processors = None

        # Average used cpu time
        # (Not relevant to workloads)
        self.average_cpu_time = None

        # Used memory
        # (Not relevant to workloads)
        self.used_memory = None

        # Number of requested processors (or nodes)
        self.requested_processors = None

        # The requested walltime
        self.requested_time = None

        # Requested memory per processor/node
        # (Not relevant since the memory model cannot be generalised in
        # Pybatsim.
        # It heavily depends on the Simgrid platform how the memory is
        # modelled)
        self.requested_memory = None

        # Whether this job's profile terminates successfully (boolean)
        self.completed = None

        # Number of the user (optional)
        self.user_id = None

        # Number of the user's group (optional)
        self.group_id = None

        # Number of the application (optional)
        self.application = None

        # Number of the job queue
        # (Not relevant to workloads)
        self.queue = None

        # Number of the partition
        # (Not relevant to workloads)
        self.partition = None

        # The preceding job
        # (Not relevant to workloads. This should be modelled instead via
        # dependencies)
        self.preceding_job = None

        # Number of seconds since the preceding job
        # (Not relevant to workloads)
        self.think_time = None

        # Additional comment field
        self.comment = None

        # Additional data field
        self.data = None

        # Additional fields known by PyBatsim
        self.deps = None

        # Update fields with values in parameters
        self.__dict__.update(kwargs)

    def process(self, random):
        keys = [k for k, _ in self.__dict__.items()]
        keys.sort()  # Prevent changing order for random generator

        for k in keys:
            v = self.__dict__[k]
            if callable(v):
                self.__dict__[k] = v(random)

    @property
    def fields_to_export(self):
        return [
            "job_number",
            # "submit_time", # Exported as subtime
            "wait_time",
            "run_time",
            "used_processors",
            "average_cpu_time",
            "used_memory",
            # "requested_processors", # Exported as res
            # "requested_time", # Exported as walltime
            "requested_memory",
            "completed",
            "user_id",
            "group_id",
            "application",
            "queue",
            "partition",
            "preceding_job",
            "think_time",
            "comment",
            "data",
            "deps",
        ]

    def copy_job(self):
        return copy.deepcopy(self)

    def submit(self, model, random, parameters, workload):
        assert None not in [
            self.submit_time,
            self.requested_time,
            self.requested_processors,
        ], "Job misses required fields: " + str(self)

        profile = model.configure_profile(random, parameters, self)
        assert profile is not None

        kwargs = {}
        for field in (self.fields_to_export +
                      model.additional_job_fields_to_export):
            try:
                if self.__dict__[field] is not None:
                    kwargs[field] = self.__dict__[field]
            except KeyError:
                pass

        workload.new_job(id=self.job_number,
                         subtime=self.submit_time,
                         walltime=self.requested_time,
                         res=self.requested_processors,
                         profile=profile,
                         **kwargs)

    def __str__(self):
        return "<Job {}>".format(
            ", ".join("{}:{}".format(k, v) for k, v in self.__dict__.items()
                      if not k.startswith("_") and v is not None))


class Option:

    def __init__(self, description=None, default=None):
        self.description = description
        self.default = default


class WorkloadModel:

    def __init__(self, **kwargs):
        self.parameters = self.merge_parameters(copy.deepcopy(kwargs))

    def merge_parameters(self, parameters, base=None):
        if base is None:
            base = self.default_parameters
        base = copy.deepcopy(base)
        for k, v in base.items():
            try:
                new_v = parameters[k]
                if new_v.description is not None:
                    v.description = new_v.description
                if new_v.default is not None:
                    v.default = new_v.default
            except KeyError:
                pass
        for k, v in parameters.items():
            if k not in base:
                base[k] = v
        return base

    def apply_parameters(self, parameters, base=None):
        if base is None:
            base = self.default_parameters
        base = copy.deepcopy(base)
        kwargs = {}
        for k, v in base.items():
            kwargs[k] = v.default
        for k, v in parameters.items():
            if k not in kwargs:
                raise ValueError(
                    "Option parameter '{}' is unknown. Use -H to list valid options.".format(k))
            else:
                kwargs[k] = v

        return kwargs

    def configure_profile(self, random, parameters, job):
        completed = job.completed if job.completed is not None else True

        return (
            Profiles.Delay(
                job.run_time or increment_float(
                    job.requested_time, -0.00000000001, True),
                ret=0 if completed else 1))

    @property
    def default_parameters(self):
        return {
            "random_seed": Option("Seed for the random generator, None means that the current time is used"),
            "num_machines": Option(
                "Number of machines to generate",
                32),
            "num_jobs": Option(
                "Number of jobs to generate",
                100),
        }

    @property
    def additional_job_fields_to_export(self):
        return []

    def create_jobs(self, random, parameters):
        raise NotImplementedError()

    def generate(self, name, description=None, date=None, verbose=0,
                 **kwargs):
        if isinstance(verbose, bool):
            verbose = 1

        parameters = self.apply_parameters(copy.deepcopy(kwargs))

        if verbose >= 1:
            print("Using workload model: {}".format(self.__class__.__name__),
                  file=sys.stderr)

        seed = parameters.get("random_seed")
        if seed is None:
            seed = time.time()
        if verbose >= 2:
            print("Using random seed: {}".format(seed), file=sys.stderr)

        r = random.Random()
        r.seed(seed)

        workload = WorkloadDescription(
            name=name,
            nb_res=int(parameters["num_machines"]),
            description=description,
            date=date,
            random_seed=seed,
            model_class=self.__class__.__name__,
            source="Workload generated by Pybatsim model generator")

        if verbose >= 1:
            print("Generating jobs for chosen workload model", file=sys.stderr)
        jobs = self.create_jobs(r, parameters) or []

        for job in jobs:
            if verbose >= 2:
                print("Generating job: {}".format(job), file=sys.stderr)
            job.submit(self, r, parameters, workload)

        if verbose >= 1:
            print("Finalising workload...", file=sys.stderr)
        workload.prepare()
        return workload

    def print_options_help(self):
        print("Options:\n")
        for k, v in self.default_parameters.items():
            print("  {}: {} (default: {})\n".format(
                k, v.description or "", v.default))

    @classmethod
    def as_main(cls, args=sys.argv[1:]):
        debug = False
        try:
            model = cls()

            parser = argparse.ArgumentParser(
                description="Generates a workload using the model: '{}'"
                .format(cls.__name__)
            )
            parser.add_argument(
                "--name", "-n",
                help="Sets the name of this workload",
                default="generated_workload", type=str
            )
            parser.add_argument(
                "--description", "-d",
                help="Sets the description of this workload",
                default="Generated workload", type=str
            )
            parser.add_argument(
                "--date", "-i",
                help="Sets the date of this workload",
                default=None, type=str
            )
            parser.add_argument(
                "--output", "-t",
                help="Sets the output file (otherwise stdout is used)",
                default=None, type=str
            )
            parser.add_argument(
                "--option",
                "-o",
                help="Sets additional options of this workload. Use -H to list the known options.",
                default=[],
                type=str,
                action='append')
            parser.add_argument(
                "--options", "-O",
                help="Sets all options from json string. Use -H to list the known options.",
                default=None, type=str)
            parser.add_argument(
                "--verbose", "-v",
                help="Increase verbosity",
                action="count", default=0
            )
            parser.add_argument(
                "--debug", "-D",
                help="Debug output in case of error",
                action="store_true"
            )
            parser.add_argument(
                "--options-help", "-H",
                help="Print the known option parameters",
                action="store_true"
            )
            args = parser.parse_args(args)
            debug = args.debug

            if args.options:
                try:
                    options = json.loads(args.options)
                except ValueError:
                    raise ValueError(
                        "Invalid json data: {}".format(
                            args.options))
            else:
                options = {}

            for option in args.option:
                option = option.split("=")
                key = option[0]
                if len(option) == 1:
                    value = True
                else:
                    value = "=".join(option[1:])
                    try:
                        value = float(value)
                    except ValueError:
                        pass
                options[key] = value

            if args.options_help:
                model.print_options_help()
                sys.exit(0)

            workload = model.generate(
                args.name,
                args.description,
                args.date,
                args.verbose,
                **options)

            output = args.output
            if output is not None:
                if args.verbose >= 1:
                    print(
                        "Writing workload to file: {}".format(output),
                        file=sys.stderr)
                with open(output, "w") as output:
                    workload.print(output)
            else:
                if args.verbose >= 1:
                    print("Writing workload to stdout", file=sys.stderr)
                workload.print()
        except Exception as e:
            if debug:
                raise
            else:
                print("Error occurred while generating workload: {}".format(e),
                      file=sys.stderr)
                sys.exit(1)
