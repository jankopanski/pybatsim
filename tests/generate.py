"""
Generate experiments to test Pybatsim.
"""
import sys
import os
import os.path
import json
import copy


def generate_energy(workloads_basedir, platforms_basedir, options):
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
        "verbosity":10,
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
         "name": "easyEnergyBudget", "verbosity": 10, "protection": True,
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
        "batsim_bin": "tests/run_batsim.sh",
        "platform": os.path.join(platforms_basedir, "energy_platform_homogeneous_no_net.xml"),
        "workload": w,
        # where all output files (stdins, stderrs, csvs...) will be outputed.
        "output_dir": "SELF",
        # if set to "SELF" then output on the same dir as this option file.
        "batsim": {
            "export": "out",        # The export filename prefix used to generate simulation output
            "energy-plugin": True,  # Enables energy-aware experiments
            "disable-schedule-tracing": True,  # remove paje output
            "verbosity": "information"  # Sets the Batsim verbosity level. Available values
                                        # are : quiet, network-only,
                                        # information (default), debug.
        },
        "scheduler": copy.deepcopy(s)
    } for s in schedulers for w in workloads_to_use]


def generate_sched_static(workloads_basedir, platforms_basedir, options):
    schedulers = []

    schedulers += [
        {
            "name_expe": "sched_delayProfilesAsTasks",
            "name": "delayProfilesAsTasks",
            "verbosity": 0,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
        {
            "name_expe": "sched_fillerSched",
            "name": "fillerSched2",
            "verbosity": 0,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
        {
            "name_expe": "sched_backfilling",
            "name": "easyBackfill2",
            "verbosity": 0,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
    ]

    workloads_to_use = [
        os.path.join(workloads_basedir, "simple_delay_workload.json")]

    options += [{
        "batsim_bin": "tests/run_batsim.sh",
        "platform": os.path.join(platforms_basedir, "simple_coalloc_platform.xml"),
        "workload": w,
        # where all output files (stdins, stderrs, csvs...) will be outputed.
        "output_dir": "SELF",
        # if set to "SELF" then output on the same dir as this option file.
        "batsim": {
            "pfs-host": "lcst_host",
            "hpst-host": "hpst_host",
            "config-file": "tests/config_noredis_dynamic.json",
            "export": "out",        # The export filename prefix used to generate simulation output
            "energy-plugin": False,  # Enables energy-aware experiments
            "disable-schedule-tracing": True,  # remove paje output
            "verbosity": "information"  # Sets the Batsim verbosity level. Available values
                                        # are : quiet, network-only,
                                        # information (default), debug.
        },
        "scheduler": copy.deepcopy(s)
    } for s in schedulers for w in workloads_to_use]


def generate_sched_script(workloads_basedir, platforms_basedir, options):
    schedulers = []

    schedulers += [
        {
            "name_expe": "sched_fillerSched",
            "name": "fillerSched2",
            "verbosity": 0,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
        {
            "name_expe": "sched_backfilling",
            "name": "easyBackfill2",
            "verbosity": 0,
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
        "batsim_bin": "tests/run_batsim.sh",
        "platform": os.path.join(platforms_basedir, "simple_coalloc_platform.xml"),
        "workload_script": {
            "path": w,
        },
        # where all output files (stdins, stderrs, csvs...) will be outputed.
        "output_dir": "SELF",
        # if set to "SELF" then output on the same dir as this option file.
        "batsim": {
            "pfs-host": "lcst_host",
            "hpst-host": "hpst_host",
            "config-file": "tests/config_noredis_dynamic.json",
            "export": "out",        # The export filename prefix used to generate simulation output
            "energy-plugin": False,  # Enables energy-aware experiments
            "disable-schedule-tracing": True,  # remove paje output
            "verbosity": "information"  # Sets the Batsim verbosity level. Available values
                                        # are : quiet, network-only,
                                        # information (default), debug.
        },
        "scheduler": copy.deepcopy(s)
    } for s in schedulers for w in workloads_to_use]


def generate_sched_dynamic(workloads_basedir, platforms_basedir, options):
    schedulers = []

    schedulers += [
        {
            "name_expe": "sched_dynamic",
            "name": "tests/schedulers/dynamicTestScheduler.py",
            "verbosity": 0,
            "protection": True,
            "interpreter": "coverage",
            "options": {
            }
        },
    ]

    options += [{
        "batsim_bin": "tests/run_batsim.sh",
        "platform": os.path.join(platforms_basedir, "simple_coalloc_platform.xml"),
        # where all output files (stdins, stderrs, csvs...) will be outputed.
        "output_dir": "SELF",
        # if set to "SELF" then output on the same dir as this option file.
        "batsim": {
            "pfs-host": "lcst_host",
            "hpst-host": "hpst_host",
            "config-file": "tests/config_noredis_dynamic.json",
            "export": "out",        # The export filename prefix used to generate simulation output
            "energy-plugin": False,  # Enables energy-aware experiments
            "disable-schedule-tracing": True,  # remove paje output
            "verbosity": "information"  # Sets the Batsim verbosity level. Available values
                                        # are : quiet, network-only,
                                        # information (default), debug.
        },
        "scheduler": copy.deepcopy(s)
    } for s in schedulers]


def generate_sched(workloads_basedir, platforms_basedir, options):
    generate_sched_static(workloads_basedir, platforms_basedir, options)
    generate_sched_script(workloads_basedir, platforms_basedir, options)
    generate_sched_dynamic(workloads_basedir, platforms_basedir, options)


def do_generate(options):
    for opt in options:
        try:
            workload_name = opt["workload"]
        except KeyError:
            try:
                workload_name = opt["workload_script"]["path"]
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

    energy = False
    sched = False
    workloads_basedir = "../../workload_profiles"
    platforms_basedir = "../../platforms"

    for arg in args:
        if arg == "--energy":
            energy = True
        elif arg == "--sched":
            sched = True
        elif arg == "--all":
            energy = True
            sched = True
        elif arg.startswith("--workloads_basedir="):
            workloads_basedir = arg.split("=")[1]
        elif arg.startswith("--platforms_basedir="):
            platforms_basedir = arg.split("=")[1]
        else:
            print("Unknown argument: {}".format(arg))
            return 1

    if not energy and not sched:
        energy = True
        sched = True

    if energy:
        generate_energy(workloads_basedir, platforms_basedir, options)

    if sched:
        generate_sched(workloads_basedir, platforms_basedir, options)

    do_generate(options)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
