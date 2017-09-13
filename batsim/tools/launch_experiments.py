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


def is_executable(file):
    return os.access(file, os.X_OK)


def run_workload_script(options):
    script = options["workload_script"]["path"]
    interpreter = options["workload_script"].get("interpreter", None)
    args = [str(s) for s in options["workload_script"].get("args", [])]

    def do_run_script(cmds):
        out_workload_file_path = os.path.join(
            options["output_dir"], "workload.json")
        with open(out_workload_file_path, "w") as f:
            script_exec = subprocess.Popen(cmds, stdout=f)
            ret = script_exec.wait()
            if ret != 0:
                raise ValueError(
                    "Workload script {} failed with return code {}".format(
                        script, ret))
        return out_workload_file_path

    if not os.path.exists(script):
        raise ValueError("Workload script {} does not exist".format(script))

    if interpreter:
        return do_run_script([interpreter, script] + args)
    else:
        if is_executable(script):
            return do_run_script(["./" + script] + args)
        elif script.endswith(".py"):
            return do_run_script(["python", script] + args)
        else:
            raise ValueError(
                "Workload script {} is not executable but also does not seem to be a python script.".format(script))


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

    workload = None
    if "workload_script" in options:
        workload = run_workload_script(options)
    else:
        try:
            workload = options["workload"]
        except KeyError:
            pass

    if workload:
        batsim_cl.append('--workload=' + workload)

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


def truncate_string(s, len_s=45):
    if len(s) > len_s:
        return "..." + s[len(s) - len_s:]
    return s


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

    if verbose:
        print("Starting batsim:", end="")
        print(" ".join(
            batsim_cl +
            [">", str(batsim_stdout_file.name),
             "2>", str(batsim_stderr_file.name)]))

    batsim_exec = subprocess.Popen(
        batsim_cl, stdout=batsim_stdout_file, stderr=batsim_stderr_file)

    if verbose:
        print("Starting scheduler:", end="")
        print(" ".join(sched_cl + [">", str(sched_stdout_file.name), "2>",
                                   str(sched_stderr_file.name)]))

    sched_exec = subprocess.Popen(
        sched_cl, stdout=sched_stdout_file, stderr=sched_stderr_file)

    if verbose:
        print("Simulation is in progress", end="")

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

    if sched_exec.poll() is not None and sched_exec.returncode != 0 and batsim_exec.poll() is None:
        print("Scheduler has died => Terminating batsim")
        batsim_exec.terminate()

    if batsim_exec.poll() is not None and batsim_exec.returncode != 0 and sched_exec.poll() is None:
        print("Batsim has died => Terminating the scheduler")
        sched_exec.terminate()

    sched_exec.wait()
    batsim_exec.wait()

    ret_code = abs(batsim_exec.returncode) + abs(sched_exec.returncode)

    if verbose or ret_code != 0:
        print("Simulation has ended")

        check_print(
            "Excerpt of log: " +
            truncate_string(
                batsim_stdout_file.name),
            tail(
                batsim_stdout_file.name,
                10))
        check_print(
            "Excerpt of log: " +
            truncate_string(
                batsim_stderr_file.name),
            tail(
                batsim_stderr_file.name,
                10))
        check_print(
            "Excerpt of log: " +
            truncate_string(
                sched_stdout_file.name),
            tail(
                sched_stdout_file.name,
                10))
        check_print(
            "Excerpt of log: " +
            truncate_string(
                sched_stderr_file.name),
            tail(
                sched_stderr_file.name,
                10))
        print_separator()

        print("Scheduler return code: " + str(sched_exec.returncode))
        print("Batsim return code: " + str(batsim_exec.returncode))

    return ret_code


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

    print("Running experiment: {}".format(options_file))

    return launch_expe(options, verbose=verbose)


if __name__ == "__main__":
    sys.exit(main())
