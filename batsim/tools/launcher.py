'''
Run PyBatsim Schedulers.

Usage:
    launcher.py <scheduler> [-o <options_string>] [options]

Options:
    -h --help                               Show this help message and exit.
    -v --verbose                            Be verbose.
    -p --protect                            Protect the scheduler using a validating machine.
    -s --socket-endpoint=<endpoint>         Batsim socket endpoint to use [default: tcp://*:28000]
    -e --event-socket-endpoint=<endpoint>   Socket endpoint to use to publish scheduler events [default: tcp://*:28001]
    -o --options=<options_string>           A Json string to pass to the scheduler [default: {}]
    -t --timeout=<timeout>                  How long to wait for responses from Batsim [default: 2000]
'''

import json
import sys
import time
import types
from datetime import timedelta
import importlib.util
import os.path

from batsim.batsim import Batsim, BatsimScheduler, NetworkHandler
from batsim.validatingmachine import ValidatingMachine

from docopt import docopt
import zmq


def module_to_class(module):
    """
    transform fooBar to FooBar
    """
    return (module[0]).upper() + module[1:]


def filename_to_module(fn):
    return str(fn).split(".")[0]


def instanciate_scheduler(name, options):
    # A scheduler module in the package "schedulers" is expected.
    if "." not in name and "/" not in name:
        my_module = name  # filename_to_module(my_filename)
        my_class = module_to_class(my_module)

        # load module(or file)
        package = __import__('schedulers', fromlist=[my_module])
        if my_module not in package.__dict__:
            print("No such scheduler (module file not found).")
            sys.exit(1)
        if my_class not in package.__dict__[my_module].__dict__:
            print("No such scheduler (class within the module file not found).")
            sys.exit(1)
        # load the class
        scheduler_non_instancied = package.__dict__[
            my_module].__dict__[my_class]

    # A full file path to the scheduler is expected
    else:
        # Add path to allow relative imports in the scheduler implementation
        sys.path.insert(0, os.path.abspath(os.path.dirname(name)))
        sys.path.insert(
            0, os.path.abspath(
                os.path.dirname(
                    os.path.dirname(name))))

        package_path = os.path.split(os.path.dirname(name))[1]
        if package_path:
            package_path = [package_path]
        else:
            package_path = []
        module_name = os.path.basename(name).split(".")[0]
        module_path = ".".join(package_path + [module_name])

        my_class = module_to_class(module_name)

        # Try to load the module with the outer package
        try:
            mod = importlib.import_module(module_path)
        # Try to load only the module as fallback
        except ModuleNotFoundError:
            spec = importlib.util.spec_from_file_location(
                "schedulers." + module_name, name)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

        del sys.path[1]
        del sys.path[0]

        try:
            scheduler_non_instancied = mod.__dict__[my_class]
        except KeyError:
            print("Module {} contains no scheduler named {}".format(
                mod, my_class))
            sys.exit(1)

    if isinstance(scheduler_non_instancied, types.FunctionType):
        from batsim.sched import as_scheduler
        scheduler = as_scheduler()(scheduler_non_instancied)
        scheduler = scheduler(options)
    else:
        scheduler = scheduler_non_instancied(options)
    if not isinstance(scheduler, BatsimScheduler):
        scheduler = scheduler()
    return scheduler


def launch_scheduler(scheduler,
                     socket_endpoint="tcp://*:28000",
                     event_socket_endpoint="tcp://*:28001",
                     options={},
                     timeout=2000,
                     protect=True,
                     verbose=0):

    if protect:
        vm = ValidatingMachine
    else:
        vm = None

    print("Scheduler: {} ({})".format(scheduler.__class__.__name__, options))
    time_start = time.time()

    try:
        bs = Batsim(scheduler,
                    NetworkHandler(socket_endpoint, verbose, timeout),
                    NetworkHandler(event_socket_endpoint, type=zmq.PUB),
                    validatingmachine=vm)
        aborted = False
        try:
            bs.start()
        except KeyboardInterrupt:
            print("Aborted...")
            aborted = True
        time_ran = str(timedelta(seconds=time.time() - time_start))
        print("Simulation ran for: " + time_ran)
        print("Job received:", bs.nb_jobs_received,
              ", scheduled:", bs.nb_jobs_scheduled,
              ", rejected:", bs.nb_jobs_rejected,
              ", killed:", bs.nb_jobs_killed,
              ", submitted:", bs.nb_jobs_submitted,
              ", changed:", len(bs.jobs_manually_changed),
              ", timeout:", bs.nb_jobs_timeout,
              ", success:", bs.nb_jobs_completed)

        if bs.nb_jobs_received != (
                bs.nb_jobs_scheduled + bs.nb_jobs_rejected +
                len(bs.jobs_manually_changed)):
            return 1
        return 1 if aborted else 0
    except KeyboardInterrupt:
        print("Aborted...")
        return 1
    return 0


def launch_scheduler_main(
        scheduler_class,
        argv=None,
        standalone=True,
        **kwargs):
    for arg in argv or sys.argv[1:]:
        if arg == "--protect":
            kwargs["protect"] = True
        elif arg == "--verbose":
            kwargs["verbose"] = 999
        elif arg.startswith("--options="):
            kwargs["options"] = json.loads(arg[arg.index("=") + 1:])
        elif arg.startswith("--timeout="):
            kwargs["timeout"] = int(arg[arg.index("=") + 1:])
        elif arg.startswith("--socket-endpoint="):
            kwargs["socket_endpoint"] = int(arg[arg.index("=") + 1:])
        elif arg.startswith("--event-socket-endpoint="):
            kwargs["event_socket_endpoint"] = int(arg[arg.index("=") + 1:])
        else:
            print("Invalid argument: {}".format(arg))
    scheduler = scheduler_class(options)

    ret = launch_scheduler(scheduler, **kwargs)

    if standalone:
        sys.exit(ret)
    else:
        if ret != 0:
            raise ValueError(
                "Scheduler exited with return code: {}".format(ret))


def main():
    # Retrieve arguments
    arguments = docopt(__doc__, version='1.0.0rc2')

    if arguments['--verbose']:
        verbose = 999
    else:
        verbose = 0

    timeout = int(arguments['--timeout'] or 2000)

    protect = bool(arguments['--protect'])

    options = json.loads(arguments['--options'])

    scheduler_filename = arguments['<scheduler>']
    socket_endpoint = arguments['--socket-endpoint']
    event_socket_endpoint = arguments['--event-socket-endpoint']

    scheduler = instanciate_scheduler(scheduler_filename, options=options)

    return launch_scheduler(scheduler,
                            socket_endpoint,
                            event_socket_endpoint,
                            options,
                            timeout,
                            protect,
                            verbose)


if __name__ == "__main__":
    sys.exit(main())
