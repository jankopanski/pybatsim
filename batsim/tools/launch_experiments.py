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
import signal


def is_executable(file):
    return os.access(file, os.X_OK)


def get_value(options, keys, fallback_keys=None, default=None):
    original_options = options

    if not isinstance(keys, list):
        keys = [keys]

    try:
        for k in keys:
            options = options[k]
    except KeyError:
        if fallback_keys:
            return get_value(original_options, fallback_keys, default=default)
        elif default is not None:
            return default
        else:
            print(
                "Option is missing in experiment settings ({})".format(
                    ".".join(keys)),
                file=sys.stderr)
            sys.exit(1)
    return options


def delete_key(options, keys):
    if not isinstance(keys, list):
        keys = [keys]

    try:
        for k in keys[:-1]:
            options = options[k]
        del options[keys[-1]]
    except KeyError:
        pass


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


def run_workload_script(options):
    script = options["batsim"]["workload-script"]["path"]
    interpreter = options["batsim"]["workload-script"].get("interpreter", None)
    args = [str(s) for s in options["batsim"]
            ["workload-script"].get("args", [])]

    def do_run_script(cmds):
        out_workload_file_path = os.path.join(
            options["output-dir"], "workload.json")
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


def generate_config(options):
    out_config_file_path = os.path.join(
        options["output-dir"], "config.json")
    with open(out_config_file_path, "w") as f:
        f.write(json.dumps(options["batsim"]["config"], indent=4))
    return out_config_file_path


def prepare_batsim_cl(options):
    if "batsim" not in options:
        print("batsim section is missing in experiment settings", file=sys.stderr)
        sys.exit(1)

    batsim_cl = [
        get_value(
            options, [
                "batsim", "executable", "path"], [
                "batsim", "bin"], default="batsim")]
    batsim_cl += get_value(options, ["batsim",
                                     "executable", "args"], default=[])

    delete_key(options, ["batsim", "executable"])
    delete_key(options, ["batsim", "bin"])

    batsim_cl.append(
        '--export=' +
        os.path.join(
            options["output-dir"],
            options.get("export", "out")))

    if "workload-script" in options["batsim"]:
        options["batsim"]["workload"] = run_workload_script(options)
        delete_key(options, ["batsim", "workload-script"])

    if "config" in options["batsim"]:
        options["batsim"]["config-file"] = generate_config(options)
        delete_key(options, ["batsim", "config"])

    for key, val in options.get("batsim", {}).items():
        if not key.startswith("_"):
            if isinstance(val, bool):
                if not val:
                    continue
                val = ""
            else:
                val = "=" + str(val)
            batsim_cl.append("--" + key + val)

    return batsim_cl


def prepare_scheduler_cl(options):
    if "scheduler" not in options:
        print(
            "scheduler section is missing in experiment settings",
            file=sys.stderr)
        sys.exit(1)

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
    sched_options["export-prefix"] = os.path.join(
        options["output-dir"], options.get("export", "out"))

    sched_cl.append("-o")
    sched_cl.append(json.dumps(sched_options))

    if options["scheduler"].get("verbose", False):
        sched_cl.append('-v')

    if options["scheduler"].get("protection", False):
        sched_cl.append('-p')

    if "socket-endpoint" in options["scheduler"]:
        sched_cl.append('-s')
        sched_cl.append(options["scheduler"]["socket-endpoint"])

    if "timeout" in options["scheduler"]:
        sched_cl.append('-t')
        sched_cl.append(options["scheduler"]["timeout"])

    return sched_cl


def launch_expe(options, verbose=True):
    if options.get("output-dir", "SELF") == "SELF":
        options["output-dir"] = os.path.dirname("./" + options["options-file"])
    if not os.path.exists(options["output-dir"]):
        os.makedirs(options["output-dir"])

    batsim_cl = prepare_batsim_cl(options)
    sched_cl = prepare_scheduler_cl(options)

    with open(os.path.join(options["output-dir"], "batsim.stdout"), "w") as batsim_stdout_file, \
            open(os.path.join(options["output-dir"], "batsim.stderr"), "w") as batsim_stderr_file, \
            open(os.path.join(options["output-dir"], "sched.stdout"), "w") as sched_stdout_file, \
            open(os.path.join(options["output-dir"], "sched.stderr"), "w") as sched_stderr_file:
        if verbose:
            print("Starting batsim: ", end="")
            print(" ".join(
                batsim_cl +
                [">", str(batsim_stdout_file.name),
                 "2>", str(batsim_stderr_file.name)]))

        try:
            batsim_exec = subprocess.Popen(
                batsim_cl, stdout=batsim_stdout_file,
                stderr=batsim_stderr_file, preexec_fn=os.setsid)
        except PermissionError:
            print(
                "Failed to run batsim: {}".format(
                    " ".join(batsim_cl)),
                file=sys.stderr)
            sys.exit(1)

        if verbose:
            print("Starting scheduler: ", end="")
            print(" ".join(sched_cl + [">", str(sched_stdout_file.name), "2>",
                                       str(sched_stderr_file.name)]))

        try:
            sched_exec = subprocess.Popen(
                sched_cl, stdout=sched_stdout_file, stderr=sched_stderr_file,
                preexec_fn=os.setsid)
        except PermissionError:
            print(
                "Failed to run the scheduler: {}".format(
                    " ".join(sched_cl)),
                file=sys.stderr)
            print("Terminating batsim", file=sys.stderr)
            os.killpg(os.getpgid(batsim_exec.pid), signal.SIGTERM)
            batsim_exec.wait()
            sys.exit(2)

        try:
            if verbose:
                print("Simulation is in progress.", end="", flush=True)

            while True:
                if batsim_exec.poll() is not None:
                    if verbose:
                        print()
                    break
                elif sched_exec.poll() is not None:
                    if verbose:
                        print()
                    break
                time.sleep(0.5)
                if verbose:
                    print(".", end="", flush=True)
        except KeyboardInterrupt:
            print("\nSimulation was aborted => Terminating batsim and the scheduler")
            os.killpg(os.getpgid(batsim_exec.pid), signal.SIGTERM)
            os.killpg(os.getpgid(sched_exec.pid), signal.SIGTERM)

        if sched_exec.poll() is not None and sched_exec.returncode != 0 and batsim_exec.poll() is None:
            print("Scheduler has died => Terminating batsim")
            os.killpg(os.getpgid(batsim_exec.pid), signal.SIGTERM)

        if batsim_exec.poll() is not None and batsim_exec.returncode != 0 and sched_exec.poll() is None:
            print("Batsim has died => Terminating the scheduler")
            os.killpg(os.getpgid(sched_exec.pid), signal.SIGTERM)

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

    options["options-file"] = options_file

    print("Running experiment: {}".format(options_file))

    return launch_expe(options, verbose=verbose)


if __name__ == "__main__":
    sys.exit(main())
