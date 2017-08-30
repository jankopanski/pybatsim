#!/usr/bin/env python3
'''
Run PyBatsim Sschedulers.

Usage:
    launcher.py <scheduler> [-o <options_string>] [options]

Options:
    -h --help                           Show this help message and exit.
    -v --verbose                        Be verbose.
    -p --protect                        Protect the scheduler using a validating machine.
    -s --socket-endpoint=<endpoint>     Batsim socket endpoint to use [default: tcp://*:28000]
    -o --options=<options_string>       A Json string to pass to the scheduler [default: {}]
'''

import json
import sys
import time
import types
from datetime import timedelta
import importlib.util
import os.path

from batsim.batsim import Batsim, BatsimScheduler
from batsim.sched import as_scheduler
from batsim.docopt import docopt
from batsim.validatingmachine import ValidatingMachine


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
            print("No such scheduler (module file not found).", flush=True)
            sys.exit(1)
        if my_class not in package.__dict__[my_module].__dict__:
            print("No such scheduler (class within the module file not found).", flush=True)
            sys.exit(1)
        # load the class
        scheduler_non_instancied = package.__dict__[my_module].__dict__[my_class]

    # A full file path to the scheduler is expected
    else:
        module_name = os.path.basename(name).split(".")[0]
        my_class = module_to_class(module_name)

        spec = importlib.util.spec_from_file_location("schedulers." + module_name, name)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        scheduler_non_instancied = mod.__dict__[my_class]

    if isinstance(scheduler_non_instancied, types.FunctionType):
        scheduler = as_scheduler()(scheduler_non_instancied)
        scheduler = scheduler(options)
    else:
        scheduler = scheduler_non_instancied(options)
    if not isinstance(scheduler, BatsimScheduler):
        scheduler = scheduler()
    return scheduler


if __name__ == "__main__":
    # Retrieve arguments
    arguments = docopt(__doc__, version='1.0.0rc2')

    if arguments['--verbose']:
        verbose = 999
    else:
        verbose = 0

    if arguments['--protect']:
        vm = ValidatingMachine
    else:
        vm = None

    options = json.loads(arguments['--options'])

    scheduler_filename = arguments['<scheduler>']
    socket_endpoint = arguments['--socket-endpoint']

    print("Starting simulation...", flush=True)
    print("Scheduler:", scheduler_filename, flush=True)
    print("Options:", options, flush=True)
    time_start = time.time()
    scheduler = instanciate_scheduler(scheduler_filename, options=options)

    bs = Batsim(scheduler,
                validatingmachine=vm,
                socket_endpoint=socket_endpoint,
                verbose=verbose)

    bs.start()
    time_ran = str(timedelta(seconds=time.time() - time_start))
    print("Simulation ran for: " + time_ran, flush=True)
    print("Job received:", bs.nb_jobs_received,
            ", scheduled:", bs.nb_jobs_scheduled,
            ", rejected:", bs.nb_jobs_rejected,
            ", killed:", bs.nb_jobs_killed,
            ", submitted:", bs.nb_jobs_submitted,
            ", timeout:", bs.nb_jobs_timeout,
            ", success:", bs.nb_jobs_completed, flush=True)

    if bs.nb_jobs_received != bs.nb_jobs_scheduled:
        sys.exit(1)
    sys.exit(0)
