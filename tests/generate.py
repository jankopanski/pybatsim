"""
Generate experiments to test Pybatsim.
"""
import sys
import os
import os.path
import json
import copy


def generate_basic(
        workloads_basedir,
        platforms_basedir,
        batsim_bin,
        batsim_args,
        options):
    schedulers = []

    schedulers += [
        {
            "name_expe": "basic_filler_sched_",
            "name": "fillerSched",
            "verbose": False,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
    ]

    workloads_to_use = [
        os.path.join(workloads_basedir, "test_bf.json")]

    options += [{
        # where all output files (stdins, stderrs, csvs...) will be outputed.
        "output-dir": "SELF",
        # if set to "SELF" then output on the same dir as this option file.

        "export": "out",        # The export filename prefix used to generate simulation output

        "batsim": {
            "executable": {
                "path": batsim_bin,
                "args": batsim_args.copy(),
            },
            "platform": os.path.join(platforms_basedir, "simple_coalloc_platform.xml"),
            "workload": w,
            "energy": False,  # Enables energy-aware experiments
            "disable-schedule-tracing": True,  # remove paje output
            "verbosity": "information"  # Sets the Batsim verbosity level. Available values
                                        # are : quiet, network-only,
                                        # information (default), debug.
        },
        "scheduler": copy.deepcopy(s)
    } for s in schedulers for w in workloads_to_use]


def generate_sched_static(
        workloads_basedir,
        platforms_basedir,
        batsim_bin,
        batsim_args,
        options):
    schedulers = []
    bat_args = batsim_args.copy()
    bat_args.append("--forward-profiles-on-submission")

    schedulers += [
        {
            "name_expe": "sched_fillerSched_",
            "name": "schedFiller",
            "verbose": False,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            },
            "dynamic":False
        },
        {
            "name_expe": "sched_backfilling_",
            "name": "schedEasySjfBackfill",
            "verbose": False,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            },
            "dynamic":False
        },
    ]

    workloads_to_use = [
        os.path.join(workloads_basedir, "test_delays.json")]

    options += [{
        # where all output files (stdins, stderrs, csvs...) will be outputed.
        "output-dir": "SELF",
        # if set to "SELF" then output on the same dir as this option file.

        "export": "out",        # The export filename prefix used to generate simulation output

        "batsim": {
            "executable": {
                "path": batsim_bin,
                "args": bat_args,
            },
            "platform": os.path.join(platforms_basedir, "simple_coalloc_platform.xml"),
            "workload": w,
            "energy": False,  # Enables energy-aware experiments
            "disable-schedule-tracing": True,  # remove paje output
            "verbosity": "information"  # Sets the Batsim verbosity level. Available values
                                        # are : quiet, network-only,
                                        # information (default), debug.
        },
        "scheduler": copy.deepcopy(s)
    } for s in schedulers for w in workloads_to_use]


def generate_sched_script(
        workloads_basedir,
        platforms_basedir,
        batsim_bin,
        batsim_args,
        options):
    schedulers = []
    bat_args = batsim_args.copy()
    bat_args.append("--forward-profiles-on-submission")

    schedulers += [
        {
            "name_expe": "sched_fillerSched_",
            "name": "schedFiller",
            "verbose": False,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
        {
            "name_expe": "sched_backfilling_",
            "name": "schedEasySjfBackfill",
            "verbose": False,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
    ]

    workloads_to_use = [
        os.path.join("tests/workloads", w)
        for w in ["generated_workload.py", "generated_workload2.py"]]

    options += [{
        # where all output files (stdins, stderrs, csvs...) will be outputed.
        "output-dir": "SELF",
        # if set to "SELF" then output on the same dir as this option file.

        "export": "out",        # The export filename prefix used to generate simulation output

        "batsim": {
            "executable": {
                "path": batsim_bin,
                "args": bat_args,
            },
            "platform": os.path.join(platforms_basedir, "simple_coalloc_platform.xml"),
            "workload-script": {
                "path": w,
            },
            "energy": False,  # Enables energy-aware experiments
            "disable-schedule-tracing": True,  # remove paje output
            "verbosity": "information"  # Sets the Batsim verbosity level. Available values
                                        # are : quiet, network-only,
                                        # information (default), debug.
        },
        "scheduler": copy.deepcopy(s)
    } for s in schedulers for w in workloads_to_use]


def generate_sched_dynamic(
        workloads_basedir,
        platforms_basedir,
        batsim_bin,
        batsim_args,
        options):
    schedulers = []
    bat_args = batsim_args.copy()
    bat_args.append("--forward-profiles-on-submission")
    bat_args.append("--enable-dynamic-jobs")
    bat_args.append("--acknowledge-dynamic-jobs")

    schedulers += [
        {
            "name_expe": "sched_dynamic",
            "name": "tests/schedulers/dynamicTestScheduler.py",
            "verbose": False,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
    ]

    options += [{
        # where all output files (stdins, stderrs, csvs...) will be outputed.
        "output-dir": "SELF",
        # if set to "SELF" then output on the same dir as this option file.

        "export": "out",        # The export filename prefix used to generate simulation output

        "batsim": {
            "executable": {
                "path": batsim_bin,
                "args": bat_args,
            },
            "platform": os.path.join(platforms_basedir, "simple_coalloc_platform.xml"),
            "energy": False,  # Enables energy-aware experiments
            "disable-schedule-tracing": True,  # remove paje output
            "verbosity": "information"  # Sets the Batsim verbosity level. Available values
                                        # are : quiet, network-only,
                                        # information (default), debug.
        },
        "scheduler": copy.deepcopy(s)
    } for s in schedulers]


def generate_sched(
        workloads_basedir,
        platforms_basedir,
        batsim_bin,
        batsim_args,
        options):
    generate_sched_static(
        workloads_basedir,
        platforms_basedir,
        batsim_bin,
        batsim_args,
        options)
    ''' Pyhton is not finding the path to batsim.sched.workloads
    generate_sched_script(
        workloads_basedir,
        platforms_basedir,
        batsim_bin,
        batsim_args,
        options)'''
    generate_sched_dynamic(
        workloads_basedir,
        platforms_basedir,
        batsim_bin,
        batsim_args,
        options)


def do_generate(options):
    for opt in options:
        try:
            workload_name = opt["batsim"]["workload"]
        except KeyError:
            try:
                workload_name = opt["batsim"]["workload-script"]["path"]
            except KeyError:
                workload_name = ""
        opt["scheduler"]["name_expe"] += os.path.splitext(
            os.path.basename(workload_name))[0]

        new_dir = "tests/" + opt["scheduler"]["name_expe"]
        try:
            os.makedirs(new_dir)
            print("Generating experiment: ", new_dir)
        except FileExistsError:
            print("Experiment already exists: ", new_dir)
        with open(new_dir + '/expe.json', 'w') as f:
            f.write(json.dumps(opt, indent=4))


def main(args):
    options = []

    basic = False
    sched = False

    workloads_basedir = "tests/workloads"
    platforms_basedir = "tests/platforms"
    batsim_bin = None
    batsim_args = []

    for arg in args:
        if arg == "--basic":
            basic = True
        elif arg == "--sched":
            sched = True
        elif arg.startswith("--batsim-bin="):
            batsim_bin = arg.split("=")[1]
        else:
            print("Unknown argument: {}".format(arg))
            return 1

    if not sched and not basic:
        basic = True
        sched = True

    if not batsim_bin:
        raise Exception("Command line option '--batsim-bin' was not set.")

    if basic:
        generate_basic(
            workloads_basedir,
            platforms_basedir,
            batsim_bin,
            batsim_args,
            options)

    if sched:
        generate_sched(
            workloads_basedir,
            platforms_basedir,
            batsim_bin,
            batsim_args,
            options)

    do_generate(options)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
