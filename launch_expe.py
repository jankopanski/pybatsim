import subprocess
import os
import sys
import json


def prepare_batsim_cl(options):
    batsim_cl = [options["batsim_bin"]]

    batsim_cl.append('--export')
    batsim_cl.append(options["output_dir"] + "/" + options["batsim"]["export"])

    if options["batsim"]["energy-plugin"]:
        batsim_cl.append('--energy')

    if options["batsim"]["disable-schedule-tracing"]:
        batsim_cl.append('--disable-schedule-tracing')

    batsim_cl.append('--verbosity')
    batsim_cl.append(options["batsim"]["verbosity"])

    batsim_cl.append('--platform')
    batsim_cl.append(options["platform"])
    batsim_cl.append('--workload')
    batsim_cl.append(options["workload"])

    return batsim_cl


def prepare_pybatsim_cl(options):
    interpreter = ["python"]
    if 'interpreter' in options["scheduler"]:
        if options["scheduler"]["interpreter"] == "coverage":
            interpreter = ["python", "-m", "coverage", "run", "-a"]
        elif options["scheduler"]["interpreter"] == "pypy":
            interpreter = ["pypy", "-OOO"]
        elif options["scheduler"]["interpreter"] == "profile":
            interpreter = ["python", "-m", "cProfile", "-o", "simul.cprof"]
        else:
            assert False, "Unknwon interpreter"

    sched_cl = interpreter
    sched_cl.append("launcher.py")

    sched_cl.append(options["scheduler"]["name"])

    sched_cl.append(options["workload"])

    sched_cl.append("-o")
    # no need to sanitize the json : each args are given as args in popen
    sched_cl.append(json.dumps(options["scheduler"]["options"]))

    if options["scheduler"]["verbosity"] > 0:
        sched_cl.append('-v')
    if options["scheduler"]["protection"]:
        sched_cl.append('-p')
    return sched_cl


def launch_expe(options, verbose=True):

    if options["output_dir"] == "SELF":
        options["output_dir"] = os.path.dirname("./" + options["options_file"])

    # use batsim in the PATH if it is not given in option
    if "batsim_bin" not in options:
        options["batsim_bin"] = "batsim"

    batsim_cl = prepare_batsim_cl(options)
    batsim_stdout_file = open(options["output_dir"] + "/batsim.stdout", "w")
    batsim_stderr_file = open(options["output_dir"] + "/batsim.stderr", "w")

    sched_cl = prepare_pybatsim_cl(options)
    sched_stdout_file = open(options["output_dir"] + "/sched.stdout", "w")
    sched_stderr_file = open(options["output_dir"] + "/sched.stderr", "w")

    print("Starting batsim")
    if verbose:
        print(" ".join(batsim_cl + [">", str(batsim_stdout_file.name), "2>",
                                    str(batsim_stderr_file.name)]))
    batsim_exec = subprocess.Popen(
        batsim_cl, stdout=batsim_stdout_file, stderr=batsim_stderr_file,
        shell=True)

    print("Starting scheduler")
    if verbose:
        print(" ".join(sched_cl + [">", str(sched_stdout_file.name), "2>",
                                   str(sched_stderr_file.name)]))
    sched_exec = subprocess.Popen(
        sched_cl, stdout=sched_stdout_file, stderr=sched_stderr_file,
        shell=True)

    print("Wait for the scheduler")
    sched_exec.wait()
    print("Scheduler return code: " + str(sched_exec.returncode))

    if sched_exec.returncode >= 2 and batsim_exec.poll() is None:
        print("Terminating batsim")
        batsim_exec.terminate()

    print("Wait for batsim")
    batsim_exec.wait()
    print("Batsim return code: " + str(batsim_exec.returncode))

    return abs(batsim_exec.returncode) + abs(sched_exec.returncode)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: launch_expe.py path/to/file.json")
        exit()

    options_file = sys.argv[1]

    options = json.loads(open(options_file).read())

    options["options_file"] = options_file

    exit(launch_expe(options, verbose=True))
