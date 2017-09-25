"""
Generate experiments to test Pybatsim.
"""
import sys
import os
import os.path
import json
import copy


def generate_energy(
        workloads_basedir,
        platforms_basedir,
        batsim_bin,
        batsim_args,
        options):
    schedulers = []

    budgets = [0, 2, 0.5]

    name_allow = {}
    name_allow[(True, False)] = "energyBud"
    name_allow[(True, True)] = "reducePC"
    name_allow[(False, True)] = "PC"
    name_allow[(False, False)] = "SHIT"

    name_shut = {}
    name_shut[True] = "SHUT"
    name_shut[False] = "IDLE"

    schedulers += [{
        "name_expe": "easyEnergyBudget_" + str(b) + "_" + name_allow[allow] + "_" + name_shut[shut],
        "name":"easyEnergyBudget",
        "verbose":True,
        "protection":True,
        "interpreter": "coverage",
        "options": {
            "budget_total": 100 * b * 100 + 30 * (7 - b) * 100,
            "budget_start": 10,
            "budget_end": 110,
            "allow_FCFS_jobs_to_use_budget_saved_measured": allow[0],
            "reduce_powercap_to_save_energy": allow[1],
            "monitoring_period":5,
            "power_idle": 30.0,
            "power_compute": 100.0,
            "opportunist_shutdown": shut,
            "pstate_switchon": 0,
            "pstate_switchoff": 1,
            "timeto_switchoff": 5,
            "timeto_switchon": 25
        }
    } for b in budgets for allow in [(True, False), (True, True), (False, True)] for shut in [True, False]]

    budgets = [1000 * 7 * 30 + 30 * 3 * 70, 1000 * 7 * 30]

    schedulers += [
        {"name_expe": "easyEnergyBudget_" + str(b) + "on1000_" +
         name_allow[allow] + "_" + name_shut[shut],
         "name": "easyEnergyBudget", "verbose": True, "protection": True,
         "interpreter": "coverage",
         "options":
         {"budget_total": b, "budget_start": 10, "budget_end": 1010,
          "allow_FCFS_jobs_to_use_budget_saved_measured": allow[0],
          "reduce_powercap_to_save_energy": allow[1],
          "monitoring_period": 5, "power_idle": 30.0, "power_compute": 100.0,
          "opportunist_shutdown": shut, "pstate_switchon": 0,
          "pstate_switchoff": 1, "timeto_switchoff": 5, "timeto_switchon": 25}}
        for b in budgets
        for allow in [(True, False),
                      (True, True),
                      (False, True)] for shut in [True, False]]

    workloads_to_use = [os.path.join(workloads_basedir, "stupid.json")]

    options += [{
        # where all output files (stdins, stderrs, csvs...) will be outputed.
        "output-dir": "SELF",
        # if set to "SELF" then output on the same dir as this option file.

        "export": "out",        # The export filename prefix used to generate simulation output

        "batsim": {
            "executable": {
                "path": batsim_bin,
                "args": batsim_args,
            },
            "platform": os.path.join(platforms_basedir, "energy_platform_homogeneous_no_net.xml"),
            "workload": w,
            "energy": True,  # Enables energy-aware experiments
            "disable-schedule-tracing": True,  # remove paje output
            "verbosity": "information"  # Sets the Batsim verbosity level. Available values
                                        # are : quiet, network-only,
                                        # information (default), debug.
        },
        "scheduler": copy.deepcopy(s)
    } for s in schedulers for w in workloads_to_use]


def generate_basic(
        workloads_basedir,
        platforms_basedir,
        batsim_bin,
        batsim_args,
        options):
    schedulers = []

    schedulers += [
        {
            "name_expe": "basic_filler_sched",
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
                "args": batsim_args,
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

    schedulers += [
        {
            "name_expe": "sched_delayProfilesAsTasks",
            "name": "schedDelayProfilesAsTasks",
            "verbose": False,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
        {
            "name_expe": "sched_fillerSched",
            "name": "schedFiller",
            "verbose": False,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
        {
            "name_expe": "sched_backfilling",
            "name": "schedEasySjfBackfill",
            "verbose": False,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
    ]

    workloads_to_use = [
        os.path.join(workloads_basedir, "simple_delay_workload.json")]

    options += [{
        # where all output files (stdins, stderrs, csvs...) will be outputed.
        "output-dir": "SELF",
        # if set to "SELF" then output on the same dir as this option file.

        "export": "out",        # The export filename prefix used to generate simulation output

        "batsim": {
            "executable": {
                "path": batsim_bin,
                "args": batsim_args,
            },
            "platform": os.path.join(platforms_basedir, "simple_coalloc_platform.xml"),
            "workload": w,
            "config": {
                "redis": {
                    "enabled": False,
                    "hostname": "127.0.0.1",
                    "port": 6379,
                    "prefix": "default"
                },
                "job_submission": {
                    "forward_profiles": True,
                    "from_scheduler": {
                        "enabled": True,
                        "acknowledge": True
                    }
                }
            },
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

    schedulers += [
        {
            "name_expe": "sched_fillerSched",
            "name": "schedFiller",
            "verbose": False,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
        {
            "name_expe": "sched_backfilling",
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
                "args": batsim_args,
            },
            "platform": os.path.join(platforms_basedir, "simple_coalloc_platform.xml"),
            "workload-script": {
                "path": w,
            },
            "config": {
                "redis": {
                    "enabled": False,
                    "hostname": "127.0.0.1",
                    "port": 6379,
                    "prefix": "default"
                },
                "job_submission": {
                    "forward_profiles": True,
                    "from_scheduler": {
                        "enabled": True,
                        "acknowledge": True
                    }
                }
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
                "args": batsim_args,
            },
            "platform": os.path.join(platforms_basedir, "simple_coalloc_platform.xml"),
            "config": {
                "redis": {
                    "enabled": False,
                    "hostname": "127.0.0.1",
                    "port": 6379,
                    "prefix": "default"
                },
                "job_submission": {
                    "forward_profiles": True,
                    "from_scheduler": {
                        "enabled": True,
                        "acknowledge": True
                    }
                }
            },
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
    generate_sched_script(
        workloads_basedir,
        platforms_basedir,
        batsim_bin,
        batsim_args,
        options)
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
        opt["scheduler"]["name_expe"] += "_" + os.path.splitext(
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
    energy = False
    sched = False

    workloads_basedir = "../../workload_profiles"
    platforms_basedir = "../../platforms"
    batsim_bin = None
    batsim_args = []

    for arg in args:
        if arg == "--basic":
            basic = True
        elif arg == "--energy":
            energy = True
        elif arg == "--sched":
            sched = True
        elif arg.startswith("--workloads-basedir="):
            workloads_basedir = arg.split("=")[1]
        elif arg.startswith("--platforms-basedir="):
            platforms_basedir = arg.split("=")[1]
        elif arg.startswith("--batsim-bin="):
            batsim_bin = arg.split("=")[1]
        else:
            print("Unknown argument: {}".format(arg))
            return 1

    if not energy and not sched and not basic:
        basic = True
        energy = True
        sched = True

    if not batsim_bin:
        batsim_bin = "docker"
        batsim_args = ["run", "batsim:dev"]

    if basic:
        generate_basic(
            workloads_basedir,
            platforms_basedir,
            batsim_bin,
            batsim_args,
            options)

    if energy:
        generate_energy(
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
