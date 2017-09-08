"""
    batsim.tools.launch_experiments
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Command to launch experiments.
"""

import subprocess
import os
import os.path
import sys
import json
import time


def prepare_batsim_cl(options):
    batsim_cl = [options["batsim_bin"]]

    batsim_cl.append(
        '--export=' +
        options["output_dir"] +
        "/" +
        options["batsim"]["export"])

    if options["batsim"]["energy-plugin"]:
        batsim_cl.append('--energy')

    if options["batsim"]["disable-schedule-tracing"]:
        batsim_cl.append('--disable-schedule-tracing')

    if "config-file" in options["batsim"]:
        batsim_cl.append('--config-file=' + options["batsim"]["config-file"])

    if "pfs-host" in options["batsim"]:
        batsim_cl.append('--pfs-host=' + options["batsim"]["pfs-host"])

    if "hpst-host" in options["batsim"]:
        batsim_cl.append('--hpst-host=' + options["batsim"]["hpst-host"])

    batsim_cl.append('--verbosity=' + options["batsim"]["verbosity"])

    batsim_cl.append('--platform=' + options["platform"])
    batsim_cl.append('--workload=' + options["workload"])

    return batsim_cl


def prepare_pybatsim_cl(options):
    sched_cl = []
    if 'interpreter' in options["scheduler"]:
        if options["scheduler"]["interpreter"] == "coverage":
            interpreter = ["python", "-m", "coverage", "run", "-a"]
        elif options["scheduler"]["interpreter"] == "pypy":
            interpreter = ["pypy", "-OOO"]
        elif options["scheduler"]["interpreter"] == "profile":
            interpreter = ["python", "-m", "cProfile", "-o", "simul.cprof"]
        else:
            assert False, "Unknown interpreter"
        sched_cl += interpreter
        sched_cl.append("launcher.py")
    else:
        sched_cl.append("pybatsim")

    sched_cl.append(options["scheduler"]["name"])

    try:
        sched_options = options["scheduler"]["options"]
    except KeyError:
        sched_options = {}
    sched_options["export_prefix"] = options["output_dir"] + "/" + options["batsim"]["export"]

    sched_cl.append("-o")
    # no need to sanitize the json : each args are given as args in popen
    sched_cl.append(json.dumps(sched_options))

    if options["scheduler"]["verbosity"] > 0:
        sched_cl.append('-v')
    if options["scheduler"]["protection"]:
        sched_cl.append('-p')
    return sched_cl


def tail(f, n):
    return subprocess.check_output(["tail", "-n", str(n), f]).decode("utf-8")


def print_separator(header=None):
    try:
        _, columns = subprocess.check_output(["stty", "size"]).split()
        columns = int(columns)
    except Exception:
        columns = None
    line = "".join(["=" for i in range(0, columns or 30)])
    if header:
        header = " " + header + " "
        line = line[:len(line) //
                    2] + header + line[len(header) + len(line) // 2:]
    print(line)


def check_print(header, s):
    if s:
        print_separator(header)
        print(s)


def launch_expe(options, verbose=True):

    if options["output_dir"] == "SELF":
        options["output_dir"] = os.path.dirname("./" + options["options_file"])
    if not os.path.exists(options["output_dir"]):
        os.makedirs(options["output_dir"])

    # use batsim in the PATH if it is not given in option
    if "batsim_bin" not in options:
        options["batsim_bin"] = "batsim"

    batsim_cl = prepare_batsim_cl(options)
    batsim_stdout_file = open(options["output_dir"] + "/batsim.stdout", "w")
    batsim_stderr_file = open(options["output_dir"] + "/batsim.stderr", "w")

    sched_cl = prepare_pybatsim_cl(options)
    sched_stdout_file = open(options["output_dir"] + "/sched.stdout", "w")
    sched_stderr_file = open(options["output_dir"] + "/sched.stderr", "w")

    print("Starting batsim", end="")
    if verbose:
        print(
            ":", " ".join(
                batsim_cl +
                [">", str(batsim_stdout_file.name),
                 "2>", str(batsim_stderr_file.name)]))
    else:
        print()

    batsim_exec = subprocess.Popen(
        batsim_cl, stdout=batsim_stdout_file, stderr=batsim_stderr_file)

    print("Starting scheduler", end="")
    if verbose:
        print(":", " ".join(sched_cl + [">", str(sched_stdout_file.name), "2>",
                                        str(sched_stderr_file.name)]))
    else:
        print()

    sched_exec = subprocess.Popen(
        sched_cl, stdout=sched_stdout_file, stderr=sched_stderr_file)

    print("Simulation is in progress", end="")
    if not verbose:
        print()

    while True:
        if batsim_exec.poll() is not None:
            break
        elif sched_exec.poll() is not None:
            break
        time.sleep(0.5)
        if verbose:
            print(".", end="", flush=True)
    if verbose:
        print()

    without_incidents = True

    if sched_exec.poll() is not None and sched_exec.returncode != 0 and batsim_exec.poll() is None:
        print("Scheduler has died => Terminating batsim")
        without_incidents = False
        batsim_exec.terminate()

    if batsim_exec.poll() is not None and batsim_exec.returncode != 0 and sched_exec.poll() is None:
        print("Batsim has died => Terminating the scheduler")
        without_incidents = False
        sched_exec.terminate()

    sched_exec.wait()
    batsim_exec.wait()

    if without_incidents:
        print("Simulation has ended")

    if verbose:
        check_print("Excerpt of log: Batsim - stdout",
                    tail(batsim_stdout_file.name, 10))
        check_print("Excerpt of log: Batsim - stderr",
                    tail(batsim_stderr_file.name, 10))
        check_print("Excerpt of log: Scheduler - stdout",
                    tail(sched_stdout_file.name, 10))
        check_print("Excerpt of log: Scheduler - stderr",
                    tail(sched_stderr_file.name, 10))
        print_separator()
    print("Scheduler return code: " + str(sched_exec.returncode))
    print("Batsim return code: " + str(batsim_exec.returncode))

    return abs(batsim_exec.returncode) + abs(sched_exec.returncode)


def usage():
    print("usage: [--verbose] launch_expe.py path/to/file.json")
    return 1


def main():
    verbose = False
    options_file = None

    for arg in sys.argv[1:]:
        if arg == "--verbose":
            verbose = True
        elif not arg.startswith("-"):
            if options_file is not None:
                return usage()
            options_file = arg
        else:
            return usage()

    options = json.loads(open(options_file).read())

    options["options_file"] = options_file

    return launch_expe(options, verbose=verbose)


if __name__ == "__main__":
    sys.exit(main())
